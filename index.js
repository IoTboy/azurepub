var azure = require('azure');

var namespace = 'iotcloudeventhub';
var accessKey = 'Endpoint=sb://iotcloudeventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=M+O/bZzxZPANa1Us5LzRXso5KMVzt/3j4iazUrmGUz4=';

var retryOperations = new azure.ExponentialRetryPolicyFilter();
var serviceBusService = azure.createServiceBusService(accessKey).withFilter(retryOperations);

var topic = 'sensor';

var topicOptions = {
        MaxSizeInMegabytes: '5120',
        DefaultMessageTimeToLive: 'PT1M'
    };

serviceBusService.createTopicIfNotExists('sensor', topicOptions, function(error){
    if(!error){
        // topic was created or exists
        console.log('Topic created or exists');
    }
});

serviceBusService.deleteSubscription('sensor', 'temp', function (error) {
    if(error) {
        console.log(error);
    } else {}
      serviceBusService.createSubscription('sensor', 'temp', function(error){
          if(!error){
              // subscription created
              console.log('Subscription for temp created');
              setInterval(sendMessage, 2000);
          } else{
            if(error.statusCode == 409){
              setInterval(sendMessage, 2000);
            }

            console.log(error);
          }
      });

});

serviceBusService.deleteSubscription('sensor', 'threshold', function (error) {
    if(error) {
        console.log(error);
    } else {}
      serviceBusService.createSubscription('sensor', 'threshold', function (error){
          if(!error){
              // subscription created
              rule.create();
              console.log('Subscription with rule created');
          } else {
            console.log(error);
          }
      });

      var rule = {
          deleteDefault: function(){
              serviceBusService.deleteRule('sensor',
                  'threshold',
                  azure.Constants.ServiceBusConstants.DEFAULT_RULE_NAME,
                  rule.handleError);
          },
          create: function(){
              var ruleOptions = {
                  sqlExpressionFilter: 'sensorvalue >= 100'
              };
              rule.deleteDefault();
              serviceBusService.createRule('sensor',
                  'threshold',
                  'thresholdFilter',
                  ruleOptions,
                  rule.handleError);
          },
          handleError: function(error){
              if(error){
                  console.log(error)
              }
          }
      }

});



var messageCount = 0;
var message = {
    body: '',
    customProperties: {
        sensorid: 'sensor_132',
        sensorvalue: 75,
        timestamp: Date.now()
    }
}

function sendMessage(){
    message.customProperties.sensorid = 'sensor_132';
    message.body = 'Temperature sensor readings: ' + messageCount;
    message.customProperties.sensorvalue = Math.floor((Math.random() * 60) + 90);
    message.customProperties.timestamp = Date.now();

    serviceBusService.sendTopicMessage(topic, message, function(error) {
      if (error) {
        console.log(error);
      } else {
        console.log('Message ' + messageCount + ' sent');
        console.log('sensorvalue: ' + message.customProperties.sensorvalue);
        messageCount++;
      }
    });
}
