const kafkaclient = require("./kafkaclient");

module.exports = class KafkaProducer {
    
    constructor() {
        this.producer = kafkaclient.producer();
        this.producer.connect()
        
    }
    
    async produceMessage(body) {
        
        await this.producer.send(body);
    }
}