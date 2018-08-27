const moment = require('moment');
const jsonld = require('jsonld');
const url = require('url');
const http2 = require('http2');
const fs = require('fs');
const md5 = require('md5');

const MILLIS_THIS_YEAR = moment([moment().year()]).valueOf();

let signalGroups = [];
let response = []; // add all json-ld objects in this
let responseTemp = []; // replaces response when new message has been processed completely
let minMaxAge = 100;
let minMaxAgeTemp = 100;

// Analytic purposes
var counterNoChanges = 0;
var counterChanges = 0;
var counter = 0;
var minEndTimeSpat;

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
    	const moy = spat.spat.intersections[0].moy; 
    	const timestamp = spat.spat.intersections[0].timeStamp;
      const graphGeneratedAtString = calcTime(moy, timestamp).utc().add(1, 'hour').format("YYYY-MM-DDTHH:mm:ss.SSS") + "Z";
    	const graphGeneratedAtDate = calcTime(moy, timestamp).utc().add(1, 'hour');

      const states = spat.spat.intersections[0].states;

      // reset response for every spat message
      responseTemp = [];
      minMaxAgeTemp = 100;

      // console.log("------------------------")
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

        if (!minEndTimeSpat || minEndTimeDate.valueOf() < minEndTimeSpat.valueOf()) minEndTimeSpat = minEndTimeDate;

      	// Generate JSON-LD document
      	let doc = {
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

        // For caching
        const maxAge = (minEndTimeDate.valueOf() - graphGeneratedAtDate.valueOf())/1000; // seconds
        if (maxAge < minMaxAgeTemp) minMaxAgeTemp = maxAge;

        responseTemp.push(doc);
      }
      response = responseTemp;
      minMaxAge = minMaxAgeTemp;
      //console.log(minMaxAge);

      // Push everything to listeners SSE
      sendUpdateToListeners(response);
    }
  } catch(error) {
    console.error(data.toString());
    console.error(error);
  }
});

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
const server = http2.createSecureServer({
  key: fs.readFileSync('./keys/localhost-privkey.pem'),
  cert: fs.readFileSync('./keys/localhost-cert.pem'),
  allowHTTP1: true
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
    if (req.headers.accept.indexOf('text/event-stream') > -1) {
      // SSE
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Access-Control-Allow-Origin', '*');
      listeners.push(res);
      res.on('close', () =>  {
        listeners.splice( listeners.indexOf(res), 1 )
      });
    } else {
      // Regular request
      res.setHeader('Access-Control-Allow-Origin', '*');
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
