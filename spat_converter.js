const moment = require('moment');
const jsonld = require('jsonld');
const url = require('url');
const http2 = require('http2');
const fs = require('fs');

const MILLIS_THIS_YEAR = moment([moment().year()]).valueOf();

let signalGroups = [];

// Analytic purposes
var counterNoChanges = 0;
var counterChanges = 0;
var counter = 0;
var minEndTimeSpat;

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
    "fragmentGroup": "ex:fragmentGroup",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
  };

process.stdin.on('data', function (data) {
  try {
    if (data != null) {
      const spat =  JSON.parse(data);
    	const moy = spat.spat.intersections[0].moy; 
    	const timestamp = spat.spat.intersections[0].timeStamp;
      const graphGeneratedAtString = calcTime(moy, timestamp).utc().format("YYYY-MM-DDTHH:mm:ss.SSS") + "Z";
    	const graphGeneratedAtDate = calcTime(moy, timestamp).utc();

      const states = spat.spat.intersections[0].states;
      // console.log("------------------------")
      for (let i=0; i<states.length; i++) {
      	// Get data
      	let movementName = states[i].movementName;
      	let signalGroupNr = states[i].signalGroup;
        let signalGroupUri = "http://example.org/signalgroup/" + signalGroupNr;
      	let eventStateName = states[i]['state-time-speed'][0].eventState;
        let eventStateUri =  "http://example.org/eventstate/" + eventStateName;
        let minEndTimeString = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.minEndTime).utc().format("YYYY-MM-DDTHH:mm:ss.SSS") + "Z";
        let maxEndTimeString = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.maxEndTime).utc().format("YYYY-MM-DDTHH:mm:ss.SSS") + "Z";
        let minEndTimeDate = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.minEndTime).utc();
        let maxEndTimeDate = calcTimeWithOffset(moy, timestamp, states[i]['state-time-speed'][0].timing.maxEndTime).utc();

        if (!minEndTimeSpat || minEndTimeDate.valueOf() < minEndTimeSpat.valueOf()) minEndTimeSpat = minEndTimeDate;

      	// Generate JSON-LD document
      	let doc = {
          "@context": signalGroupContext,
      		"@id": "http://example.org/signalgroup/" + signalGroupNr + "?time=" + graphGeneratedAtString,
          "generatedAt": graphGeneratedAtString,
      		"@graph": [
  				 {
  			       "@id": "http://example.org/signalgroups/1",
  			       "@type": "ex:signalgroup",
  			       "eventState": {
                  "@id": eventStateUri,
                  "@type": "EventState",
                  "rdfs:label": eventStateName },
  			       "minEndTime": minEndTimeString,
  			       "maxEndTime": maxEndTimeString
  		     }
      		]
      	}

        // console.log(JSON.stringify(doc));

        // Check if version exists
        if(!signalGroups[signalGroupUri]){
          signalGroups[signalGroupUri] = {};
          signalGroups[signalGroupUri].jsonld = doc;
          signalGroups[signalGroupUri].minEndTimeDate = minEndTimeDate;
          signalGroups[signalGroupUri].maxEndTimeDate = maxEndTimeDate;
          signalGroups[signalGroupUri].graphGeneratedAtDate = graphGeneratedAtDate;
          signalGroups
        } else {
          let currentSignalGroup = signalGroups[signalGroupUri];
          // Check if this changes previous version
          if (eventStateName != currentSignalGroup.jsonld['@graph'][0].eventState['rdfs:label']
            || minEndTimeDate.valueOf() > currentSignalGroup.minEndTimeDate.valueOf()+2
            || maxEndTimeDate.valueOf() < currentSignalGroup.maxEndTimeDate.valueOf()-2) {
            // console.log("Previousmin:" + currentSignalGroup.minEndTimeDate.format() + "--- Now: " + minEndTimeDate.format());
            // console.log("Previousmin:" + currentSignalGroup.minEndTimeDate + "--- Now: " + minEndTimeDate);
            // console.log("Previousmax:" + currentSignalGroup.maxEndTimeDate.format() + "--- Now: " + maxEndTimeDate.format());
            // console.log("Previousmax:" + currentSignalGroup.maxEndTimeDate + "--- Now: " + maxEndTimeDate);
            // console.log("Previous state:" + currentSignalGroup.jsonld['@graph'][0].eventState['rdfs:label'] + "--- Now: " + eventStateName)
            counter++;
            // TODO Archive previous version
            // Update current version
            let newSignalGroup = {};
            newSignalGroup.jsonld = doc;
            newSignalGroup.minEndTimeDate = minEndTimeDate;
            newSignalGroup.maxEndTimeDate = maxEndTimeDate;
            newSignalGroup.graphGeneratedAtDate = graphGeneratedAtDate;
            newSignalGroup.maxAge = (minEndTimeDate.valueOf() - graphGeneratedAtDate.valueOf())/1000; // seconds
            signalGroups[signalGroupUri] = newSignalGroup;
          } else if (eventStateName != currentSignalGroup.jsonld['@graph'][0].eventState['rdfs:label']
            || minEndTimeDate.valueOf() < currentSignalGroup.minEndTimeDate.valueOf()
            || maxEndTimeDate.valueOf() > currentSignalGroup.maxEndTimeDate.valueOf()) {
            // should only happens when emergencies happen
            // should be pushed to the client
          } else {
          }
        }
      }
      // if (counter > 0) counterChanges++; else counterNoChanges++;
      // counter = 0;
      // console.log(counterChanges + " (changes)/" + counterNoChanges + " no changes");
      // console.log((minEndTimeSpat.valueOf() - graphGeneratedAtDate.valueOf())/1000)
      // minEndTimeSpat = null;
      // counter = 0;
    }
  } catch(error) {
      console.error(error);
  }
});

// timeOffset = 1/100 second (minEndTime, maxEndTime)
function calcTimeWithOffset(moy, timestamp, timeOffset) {
	return moment(MILLIS_THIS_YEAR + moy*60*1000 + timestamp + timeOffset * 100);
}

function calcTime(moy, timestamp) {
	return moment(MILLIS_THIS_YEAR + moy*60*1000 + timestamp);
}

// SERVER
const server = http2.createSecureServer({
  key: fs.readFileSync('./keys/server.key'),
  cert: fs.readFileSync('./keys/server.crt')
}, onRequest);
server.on('error', (err) => console.error(err));

server.listen(3000);

// Request handler
function onRequest (req, res) {
  const timeRequest = moment(); // when the request happens
  res.setHeader('Access-Control-Allow-Origin', '*');
  const uri = url.parse(req.url, true).query.uri;
  const signalGroup = signalGroups[uri];

  if (signalGroup) {
    // Calc max-age, relates to time of request
    res.setHeader('Content-Type', 'application/ld+json');
    res.setHeader('Cache-Control', 'public, max-age=' + Math.floor(signalGroup.maxAge));
    res.end(JSON.stringify(signalGroup.jsonld));
  } else {
    res.writeHead(404, {});
    res.end('Not found');
  }
}