const KafkaAdmin = require("./lib/kafka/kafkaadmin");
const KafkaProducer = require("./lib/kafka/kafkaproducer");
const KafkaConsumer = require("./lib/kafka/kafkaconsumer");


exports.handler = async (event, context) => {
    const Admin = new KafkaAdmin();
    const Producer = new KafkaProducer();
  
    await kafkaConsume();
};

async function kafkaConsume() {
  const consumer = new KafkaConsumer();
  await consumer.run();
}
