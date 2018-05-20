var mqtt = require('mqtt')
var client  = mqtt.connect('mqtt://146.253.51.199:30201')
 
client.on('connect', function () {
  client.subscribe('K648/spat/json', {'qos': 2})
})
 
client.on('message', function (topic, message) {
  // message is Buffer
  console.log(message.toString())
})
