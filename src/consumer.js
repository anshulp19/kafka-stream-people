const { Kafka } = require('kafkajs')
const config = require('./config')

const kafka = new Kafka({
  clientId: config.kafka.CLIENTID,
  brokers: config.kafka.BROKERS
})

const topic = config.kafka.TOPIC
const consumer = kafka.consumer({
  groupId: config.kafka.GROUPID
});

const run = async (cb) => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const jsonObj = JSON.parse(message.value.toString());
        if(jsonObj) {
          console.log(`Camera Info: ${JSON.stringify(jsonObj)}`);
          cb(jsonObj);
        }
      }
    });
  } catch(err) {
      console.error(`error => ${JSON.stringify(err)}`);
  }
}


const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
});

module.exports = {
    kafkaSubscribe: run
};