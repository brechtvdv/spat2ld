const moment = require('moment');
const jsonld = require('jsonld');
const url = require('url');
//const http2 = require('http2');
const http = require('http');
const fs = require('fs');
const md5 = require('md5');
const N3 = require('n3');

const { DataFactory } = N3;
const { namedNode, literal, defaultGraph, quad } = DataFactory;

const MILLIS_THIS_YEAR = moment([moment().year()]).valueOf();

let signalGroups = {};

process.stdin.on('data', function (data) {
  try {
    if (data != null) {
      const spat = JSON.parse(data.toString().split('\n')[0]); // sometimes multiple SPAT messages in one data object
      
      processSpat(spat);
    }
  } catch(error) {
    console.error(data.toString());
    console.error(error);
  }
});

async function processSpat(spat) {
  const writer = N3.Writer({ prefixes: {}});

  const received = moment();
  const moy = spat.spat.intersections[0].moy; 
  const timestamp = spat.spat.intersections[0].timeStamp;
  const graphGeneratedAtString = calcTime(moy, timestamp).utc().add(1, 'hour').format("YYYY-MM-DDTHH:mm:ss.SSS") + "Z";
  const graphGeneratedAtDate = calcTime(moy, timestamp).utc().add(1, 'hour');
  const graphUri = "https://opentrafficlights.org/spat/K648" + "?time=" + graphGeneratedAtString;
  const states = spat.spat.intersections[0].states;   

  writer.addQuad(quad(
    namedNode(graphUri),
    namedNode('http://www.w3.org/ns/prov#generatedAtTime'),
    literal(graphGeneratedAtString, namedNode('http://www.w3.org/2001/XMLSchema#date'))
  ));

  let hasUpdate = false; // indicates that this SPAT message contains an update

  for (let i=0; i<states.length; i++) {
    // Get data
    let movementName = states[i].movementName;
    let signalGroupNr = states[i].signalGroup;
    let signalGroupUri = "https://opentrafficlights.org/id/signalgroup/K648/" + signalGroupNr;
    let signalPhaseLabel = states[i]['state-time-speed'][0].eventState;
    let signalPhaseUri = convertSignalLabelToConcept(signalPhaseLabel);
    let minEndTimeString = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.minEndTime).utc().add(1, 'hour').format("YYYY-MM-DDTHH:mm:ss.SSS") + "Z";
    let maxEndTimeString = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.maxEndTime).utc().add(1, 'hour').format("YYYY-MM-DDTHH:mm:ss.SSS") + "Z";
    let minEndTimeDate = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.minEndTime).utc().add(1, 'hour');
    let maxEndTimeDate = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.maxEndTime).utc().add(1, 'hour');

    let min = Math.round((minEndTimeDate.valueOf() - graphGeneratedAtDate.valueOf())/1000);
    let max = Math.round((maxEndTimeDate.valueOf() - graphGeneratedAtDate.valueOf())/1000);

    // The view (label, min or max) has changed
    if (signalGroups[signalGroupUri] && 
      (signalGroups[signalGroupUri].signalPhaseLabel != signalPhaseLabel 
      || signalGroups[signalGroupUri].min != min
      || signalGroups[signalGroupUri].max != max)) {
      hasUpdate = true;

      // Generate N-Quads document for timeseries server
      writer.addQuad(quad(
        namedNode(signalGroupUri),
        namedNode('https://w3id.org/opentrafficlights#signalState'),
        writer.blank([{
            predicate: namedNode('http://www.w3.org/2000/01/rdf-schema#type'),
            object:    namedNode('https://w3id.org/opentrafficlights#SignalState')
          },
          {
            predicate: namedNode('https://w3id.org/opentrafficlights#signalPhase'),
            object:    namedNode(signalPhaseUri)
          },{
            predicate: namedNode('https://w3id.org/opentrafficlights#minEndTime'),
            object:    literal(minEndTimeString, namedNode('http://www.w3.org/2001/XMLSchema#date'))
          },{
            predicate: namedNode('https://w3id.org/opentrafficlights#maxEndTime'),
            object:    literal(maxEndTimeString, namedNode('http://www.w3.org/2001/XMLSchema#date'))
          }]),
        namedNode(graphUri)
      ));
    }
  
    if (!signalGroups[signalGroupUri]) signalGroups[signalGroupUri] = {};
    signalGroups[signalGroupUri]['min'] = min;
    signalGroups[signalGroupUri]['max'] = max;  
    signalGroups[signalGroupUri]['signalPhaseLabel'] = signalPhaseLabel;
  }

  if (hasUpdate) writer.end((error, result) => console.log(result));
}

// timeOffset = 1/100 second (minEndTime, maxEndTime)
function calcTimeWithOffset(moy, timestamp, timeOffset) {
  return moment(MILLIS_THIS_YEAR + moy*60*1000 + timestamp + timeOffset * 100);
}

function calcTime(moy, timestamp) {
  return moment(MILLIS_THIS_YEAR + moy*60*1000 + timestamp);
}

function convertSignalLabelToConcept(label) {
  switch(label) {
    case "Unavailable":
      return "https://w3id.org/opentrafficlights/thesauri/signalphase/0";
      break;
    case "Unlit (DARK)":
      return "https://w3id.org/opentrafficlights/thesauri/signalphase/1";
      break;
    case "Stop-Then-Proceed":
      return "https://w3id.org/opentrafficlights/thesauri/signalphase/2";
      break;
    case "Stop And Remain":
    case "stop-And-Remain":
      return "https://w3id.org/opentrafficlights/thesauri/signalphase/3";
      break;
    case "Pre-Movement":
      return "https://w3id.org/opentrafficlights/thesauri/signalphase/4";
      break;
    case "Permissive Movement Allowed":
      return "https://w3id.org/opentrafficlights/thesauri/signalphase/5";
      break;
    case "Protected Movement Allowed":
    case "protected-Movement-Allowed":
      return "https://w3id.org/opentrafficlights/thesauri/signalphase/6";
      break;
    case "Permissive Clearance":
      return "https://w3id.org/opentrafficlights/thesauri/signalphase/7";
      break;
    case "Protected Clearance":
      return "https://w3id.org/opentrafficlights/thesauri/signalphase/8";
      break;
    case "Caution Conflicting Traffic (Flashing)":
      return "https://w3id.org/opentrafficlights/thesauri/signalphase/9";
      break;
    default:
      return "https://w3id.org/opentrafficlights/thesauri/signalphase/0";
  }
}