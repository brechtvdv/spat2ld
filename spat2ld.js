const moment = require('moment');
const jsonld = require('jsonld');

const MILLIS_THIS_YEAR = moment([moment().year()]).valueOf();

var minEndTimeSpat; // contains smallest end time of the whole SPAT message

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
	// output spat_consumer
	try {
		if (data != null) {
			const spat =  JSON.parse(data);
      // console.log(spat)
      const moy = spat.spat.intersections[0].moy; 
      const timestamp = spat.spat.intersections[0].timeStamp;
      const graphGeneratedAtString = calcTime(moy, timestamp).utc().add(1, 'hour').format("YYYY-MM-DDTHH:mm:ss.SSS") + "Z";
      const graphGeneratedAtDate = calcTime(moy, timestamp).utc().add(1, 'hour');

      const states = spat.spat.intersections[0].states;
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
      				"rdfs:label": eventStateName 
      			},
      			"minEndTime": minEndTimeString,
      			"maxEndTime": maxEndTimeString
      		}]
  		}
  		console.log(JSON.stringify(doc));
      	}
      }
  } catch(error) {
    console.error(data.toString());
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