const { Kafka, logLevel  } = require("kafkajs");
const {
  awsIamAuthenticator,
  Type,
} = require("@jm18457/kafkajs-msk-iam-authentication-mechanism");

module.exports = new Kafka({
  brokers: process.env.KAFKA_BROKERS.split(","),
  clientId: process.env.KAFKA_CLIENT_ID,
  logLevel: logLevel.WARN,
  ssl: true,
  sasl: {
    mechanism: Type,
    authenticationProvider: awsIamAuthenticator(
      process.env.REGION,
      process.env.KAFKA_TTL
    ),
  },
});
