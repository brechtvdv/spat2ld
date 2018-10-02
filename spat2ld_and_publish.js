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
let response = []; // add all json-ld objects in this; contains latest updates
let responseEmergencies = []; // add all json-ld objects in this who are irregular with the spat spec (e.g. emergencies)
let emergenciesUpdated = false;
let responseTemp = []; // replaces response when new message has been processed completely
let minMaxAge = 100;
let minMaxAgeTemp = 100;
let minMaxAgePerSg = {};
let listeners = [];

// CONVERTER
const signalGroupContext = {
    "generatedAt": {
      "@id": "http://www.w3.org/ns/prov#generatedAtTime",
      "@type": "http://www.w3.org/2001/XMLSchema#date"
    },
    "ex": "http://example.org#",
    "EventState": "http://example.org/eventstate/",
    "eventState": {
      "@id": "ex:eventstate",
      "@type": "EventState"
    },
    "minEndTime": {
      "@id": "ex:minendtime",
      "@type": "http://www.w3.org/2001/XMLSchema#date"
    },
    "maxEndTime": {
      "@id": "ex:maxendtime",
      "@type": "http://www.w3.org/2001/XMLSchema#date"
    },
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
  };

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
  const received = moment();
  const moy = spat.spat.intersections[0].moy; 
  const timestamp = spat.spat.intersections[0].timeStamp;
  const graphGeneratedAtString = calcTime(moy, timestamp).utc().add(1, 'hour').format("YYYY-MM-DDTHH:mm:ss.SSS") + "Z";
  const graphGeneratedAtDate = calcTime(moy, timestamp).utc().add(1, 'hour');

  const states = spat.spat.intersections[0].states;   

  let responseTemp = [];
  let minMaxAgeTemp = 100;

  for (let i=0; i<states.length; i++) {
    // Get data
    let movementName = states[i].movementName;
    let signalGroupNr = states[i].signalGroup;
    let signalGroupUri = "http://example.org/signalgroup/" + signalGroupNr;
    let eventStateName = states[i]['state-time-speed'][0].eventState;
    let eventStateUri =  "http://example.org/eventstate/" + eventStateName;
    let minEndTimeString = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.minEndTime).utc().add(1, 'hour').format("YYYY-MM-DDTHH:mm:ss.SSS") + "Z";
    let maxEndTimeString = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.maxEndTime).utc().add(1, 'hour').format("YYYY-MM-DDTHH:mm:ss.SSS") + "Z";
    let minEndTimeDate = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.minEndTime).utc().add(1, 'hour');
    let maxEndTimeDate = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.maxEndTime).utc().add(1, 'hour');

    // Generate JSON-LD document
    const doc = {
      "@context": signalGroupContext,
      "@id": signalGroupUri + "?time=" + graphGeneratedAtString,
      "generatedAt": graphGeneratedAtString,
      "@graph": [
       {
           "@id": signalGroupUri,
           "@type": "ex:signalgroup",
           "eventState": {
              "@id": eventStateUri,
              "@type": "EventState",
              "rdfs:label": eventStateName,
              "minEndTime": minEndTimeString,
              "maxEndTime": maxEndTimeString }
       }
      ]
    }

    // Don't use prefixes because timeseries server concatenates it to the file everytime
    const graph = signalGroupUri + "?time=" + graphGeneratedAtString;
    
    await writeQuads(graph, graphGeneratedAtString, signalGroupUri, eventStateUri, eventStateName, minEndTimeString, maxEndTimeString);
    // For caching
    const maxAge = (minEndTimeDate.valueOf() - graphGeneratedAtDate.valueOf())/1000; // seconds
    if (maxAge < minMaxAgeTemp) minMaxAgeTemp = maxAge;
    minMaxAgePerSg[signalGroupUri] = maxAge;

    responseTemp.push(doc);

    // When emergency, the minendtime becomes earlier or maxendtime becomes longer
    if (signalGroups[signalGroupUri] && signalGroups[signalGroupUri].eventStateName === eventStateName
      && ((minEndTimeDate.valueOf() + 10) < signalGroups[signalGroupUri].minEndTimeDate.valueOf() || maxEndTimeDate.valueOf() > (signalGroups[signalGroupUri].maxEndTimeDate.valueOf() + 10))) {
      signalGroups[signalGroupUri].emergencyTimeDate = received; // time of detection
      emergenciesUpdated = true;
    }
    
    if (!signalGroups[signalGroupUri]) signalGroups[signalGroupUri] = {};

    signalGroups[signalGroupUri]['minEndTimeDate'] = minEndTimeDate;
    signalGroups[signalGroupUri]['maxEndTimeDate'] = maxEndTimeDate;  
    signalGroups[signalGroupUri]['eventStateName'] = eventStateName;
    signalGroups[signalGroupUri]['jsonld'] = doc;
  }

  response = responseTemp;
  minMaxAge = minMaxAgeTemp;

  // Push everything to listeners SSE
  sendUpdateToListeners(response);
}

async function writeQuads(graph, graphGeneratedAtString, signalGroupUri, eventStateUri, eventStateName, minEndTimeString, maxEndTimeString) {
  const writer = N3.Writer({ prefixes: {}});

  // Generate N-Quads document for timeseries server
  writer.addQuad(
    namedNode(graph),
    namedNode('http://www.w3.org/ns/prov#generatedAtTime'),
    literal(graphGeneratedAtString, namedNode('http://www.w3.org/2001/XMLSchema#date'))
  );
  writer.addQuad(quad(
    namedNode(signalGroupUri),
    namedNode('http://www.w3.org/2000/01/rdf-schema#type'),
    namedNode('http://example.org#signalgroup'),
    namedNode(graph)
  ));
  writer.addQuad(quad(
    namedNode(signalGroupUri),
    namedNode('http://example.org#eventstate'),
    namedNode(eventStateUri),
    namedNode(graph)
  ));
  writer.addQuad(quad(
    namedNode(eventStateUri),
    namedNode('http://www.w3.org/2000/01/rdf-schema#type'),
    namedNode('http://example.org#EventState'),
    namedNode(graph)
  ));
  writer.addQuad(quad(
    namedNode(eventStateUri),
    namedNode('http://www.w3.org/2000/01/rdf-schema#label'),
    literal(eventStateName, 'en'),
    namedNode(graph)
  ));
  writer.addQuad(quad(
    namedNode(eventStateUri),
    namedNode('http://example.org#minEndTime'),
    literal(minEndTimeString, namedNode('http://www.w3.org/2001/XMLSchema#date')),
    namedNode(graph)
  ));
  writer.addQuad(quad(
    namedNode(eventStateUri),
    namedNode('http://example.org#maxEndTime'),
    literal(maxEndTimeString, namedNode('http://www.w3.org/2001/XMLSchema#date')),
    namedNode(graph)
  ));

  //writer.end((error, result) => console.log(result));
}

function sendUpdateToListeners(_doc) {
  listeners.forEach((client) => {
    client.write("data: " + JSON.stringify(_doc) + '\n\n');
  });
}

// timeOffset = 1/100 second (minEndTime, maxEndTime)
function calcTimeWithOffset(moy, timestamp, timeOffset) {
	return moment(MILLIS_THIS_YEAR + moy*60*1000 + timestamp + timeOffset * 100);
}

function calcTime(moy, timestamp) {
	return moment(MILLIS_THIS_YEAR + moy*60*1000 + timestamp);
}

// SERVER
const server = http.createServer({
  /*key: fs.readFileSync('./keys/localhost-privkey.pem'),
  cert: fs.readFileSync('./keys/localhost-cert.pem')*/
}, onRequest);
server.on('error', (err) => console.error(err));
process.on('uncaughtException', function (err) {
  console.error(err.stack);
  console.log("Node NOT Exiting...");
});
server.listen(3002);

// Request handler
function onRequest (req, res) {
  try {
    res.setHeader('Access-Control-Allow-Origin', '*');
    let params = url.parse(req.url, true);
    if (req.headers.accept.indexOf('text/event-stream') > -1) {
      // SSE
      res.setHeader('Content-Type', 'text/event-stream');
      listeners.push(res);
      res.on('close', () =>  {
        listeners.splice( listeners.indexOf(res), 1 )
      });
    } else if (req.url === '/emergencies') {
      if (emergenciesUpdated) {
        emergenciesUpdated = false;
        let responseEmergenciesTemp = [];
        let now = moment().valueOf();
        // Construct response object with all emergencies in
        for (let sg in signalGroups) {
          if (typeof signalGroups[sg].emergencyTimeDate != 'undefined' && ((now - signalGroups[sg].emergencyTimeDate.valueOf()) < 16000)) {
            responseEmergenciesTemp.push(signalGroups[sg].jsonld);
          }
        }
        responseEmergencies = responseEmergenciesTemp;
      }
      res.setHeader('Content-Type', 'application/ld+json');
      let etag = 'W/"' + md5(responseEmergencies) + '"';
      res.setHeader('ETag', etag);
      res.setHeader('Cache-Control', 'public, must-revalidate');
      res.end(JSON.stringify(responseEmergencies));
    } else if (params.query.signalgroup) {
      let responseSg = [];
      let minMaxAgeSg = 20;
      let sgs = []; // list of signalgroups that match query param
      if (!Array.isArray(params.query.signalgroup)) sgs.push(params.query.signalgroup);
      else sgs = params.query.signalgroup;
      sgs.forEach((sg) => {
        if (sg['@graph'][0]['@id'] === sg) {
          console.log(sg)
          responseSG.push(sg);
          //if (minMaxAgePerSg[sg] && minMaxAgePerSg[sg] < minMaxAgeSg) minMaxAgeSg = minMaxAgePerSg[sg];
        }
      });
      console.log(responseSg);
      //res.setHeader('Cache-Control', 'public, max-age=' + Math.floor(minMaxAgeSg));
      res.setHeader('Content-Type', 'application/ld+json');
      res.end(JSON.stringify(responseSg));
    } else {
      // Regular request
      res.setHeader('Content-Type', 'application/ld+json');
      let etag = 'W/"' + md5(response) + '"';
      res.setHeader('ETag', etag);
      res.setHeader('Cache-Control', 'public, max-age=' + Math.floor(minMaxAge));
      res.end(JSON.stringify(response));
    }
  } catch (e) {
    // console.error(e);
    res.writeHead(500, {});
    res.end('Failure')
  }
}
