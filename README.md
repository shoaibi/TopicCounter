# README #

## What is this? ##
* A simple Netty HTTP Server that takes 2 parameters, topic and message.
* Netty application forwards the catched data after validation to Queue Producer
* Queue Producer talks to the queue backend(Apache Kafka) and publishes the message
* Queue Consumer keeps listening for the topic specified as cli arg
* Queue Consumer gets the message
* Queue Consume invokes TopicCounter to increment the counter for the topic of received message

## How was it done? ##
* With total insanity

## Dependencies ##
* Java 7+, running Kafka, Zookeeper and Hbase instances
* All jars are bundled

## Setup ##
* Place the code wherever you like
* Edit the constants in MessageProducer and TableManager to suit your setup. Support for xml/json files might be added in future.
* Run ```MessageConsumer.java``` with the topic name to listen to as commandline argument
* Run the ```HttpServer.java``` (you may specify a custom port to bind to as first argument)
* Make a request the host and port HttpServer bound to with topic and Message like:
```
curl "http://localhost:8080/?topic=test&message=Message+goes+here"
```

## License ##
Code is provided as is with no liability and terms whatsoever. It may turn your toaster to zombie, it may trigger doomsday device. Try at your own risk.
