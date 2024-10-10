const kafkaclient = require("./kafkaclient");


module.exports = class KafkaAdmin {
    
    constructor() {
        this.admin = kafkaclient.admin();
    }
    
    async fetchTopicMetadata(topicName) {
        await this.admin.connect()
        const topics = await this.admin.fetchTopicMetadata({topics: [topicName]})
        await this.admin.disconnect()
        
        return topics;
    }
    
    // Function to list consumer groups
    async listConsumerGroups() {
      try {
        const consumerGroups = await this.admin.listGroups();
        console.log('Consumer Groups:');
        console.log(consumerGroups);
        // consumerGroups.forEach(group => console.log(group));
      } catch (error) {
        console.error('Error listing consumer groups:', error);
      }
    };
    
    async describeConsumerGroup(groupId)  {
      try {
        const groupInfo = await this.admin.describeGroups([groupId]);
        console.log(`Consumer Group: ${groupId}`);
        // groupInfo.members.forEach(member => {
        //   console.log(`  Member ID: ${member.memberId}`);
        //   console.log(`    Client ID: ${member.clientId}`);
        //   console.log(`    Host: ${member.clientHost}`);
        //   console.log(`    Assignments: ${JSON.stringify(member.memberAssignment)}`);
        // });
      } catch (error) {
        console.error(`Error describing consumer group ${groupId}:`, error);
      }
    };
}