const kafkaclient = require("./kafkaclient");
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry");

module.exports = class KafkaConsumer {
  constructor() {
    this.messages = [];
  }

  setSchemaRegistry() {
    this.schemaRegistryClient = new SchemaRegistry({
      host: "http://localhost:8081",
    });
  }

  async getSchemaId() {
    this.setSchemaRegistry();
    const schemaId = await this.schemaRegistryClient.getRegistryId(
      "schemaSubject",
      "schemaVersion"
    );
    return schemaId;
  }

  async decodeMessage(message) {
    return this.schemaRegistryClient.decode(message);
  }

  async subscribeTopic() {
    await this.consumer.connect();

    await this.consumer.subscribe({
      topics: ["dt.orderfullfillment.sapeccslt.site.v1.avro.dev"],
    });
  }

  async run() {
    const consumerGroup = "site-id";
    const consumer = kafkaclient.consumer({
      groupId: consumerGroup,
      allowAutoTopicCreation: false,
    });

    this.setSchemaRegistry();

    const { CONNECT, DISCONNECT, GROUP_JOIN, CRASH, REQUEST_TIMEOUT, STOP, REBALANCING } =
      consumer.events;

    consumer.on(CONNECT, (e) =>
      console.info(
        `A Consumer from Consumer Group (${consumerGroup}) connected at ${e.timestamp}`
      )
    );
    
    consumer.on(DISCONNECT, (e) =>
      console.info(
        `A Consumer from Consumer Group (${consumerGroup}) disconnected at ${e.timestamp}`
      )
    );

    consumer.on(GROUP_JOIN, (e) =>
      console.info(
        `A Consumer (${e.payload.memberId}) joined the Consumer Group (${e.payload.groupId}) at ${e.timestamp}`,
        `LeaderId=${e.payload.leaderId}`
      )
    );

    consumer.on(CRASH, (e) =>
      console.error(
        `A Consumer from the Consumer Group (${consumerGroup}) crashed at ${e.timestamp}`,
        e.payload.error
      )
    );

    consumer.on(REQUEST_TIMEOUT, (e) =>
      console.warn(
        `Consumer request timed out at ${e.timestamp}`,
        JSON.stringify(e.payload)
      )
    );

    consumer.on(REBALANCING, async () => {
      console.log('Consumer group is rebalancing...');
    });
    
    consumer.on('rebalanced', async (event) => {
      console.log(`Consumer group has rebalanced: ${JSON.stringify(event)}`);
    });

    consumer.on(STOP, (e) =>
      console.info(`Consumer stopped`, JSON.stringify(e))
    );

    // this.consumer = kafkaclient.consumer({ groupId: 'site-id' });
    try {
      await consumer.connect();
      console.log("Consumer connected successfully...");
    } catch (e) {
      console.error("Consumer connection error.");
      console.log(e);
      throw e;
    }

    try {
      await consumer.subscribe({
        topic: "dt.orderfullfillment.sapeccslt.site.v1.avro.dev",
        fromBeginning: true,
      });
      console.log("Consumer subscribed to topic successfully...");
    } catch (e) {
      console.error("Consumer subscription error.");
      console.log(e);
      throw e;
    }

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log("Consumed Message: ");
        console.log({
          key: message.key.toString(),
          value: message.value.toString("hex"),
          headers: message.headers,
        });
      },
    });
  }
};
