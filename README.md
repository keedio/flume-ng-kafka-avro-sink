flume-ng-kafka-avro-sink
================

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with [kafka 0.8.0](http://kafka.apache.org/08/quickstart.html) sending Avro messages to the Kafka cluster with Avro Schema Repository Server from Camus.

Requirements
------------
- [Linkedin Camus](https://github.com/linkedin/camus)
- [Apache Kafka 0.8](https://github.com/apache/kafka)
- [Apache Flume NG 1.5.2](https://github.com/apache/flume)
- [Avro Schema Repository Server](https://github.com/buildoop/avro-schema-repo)

Compile
---------
    $ mvn clean package

Configuration of Kafka Avro Sink
----------

    agent.sinks = kafka-avro-sink
    agent.sinks.kafka-avro-sink.type = org.redoop.flume.sink.avro.kafka.KafkaAvroSink
    agent.sinks.kafka-avro-sink.channel = memory-channel
    agent.sinks.kafka-avro-sink.zk.connect = 127.0.0.1:2181
    agent.sinks.kafka-avro-sink.topic = avrotopic
    agent.sinks.kafka-avro-sink.batchsize = 200
    agent.sinks.kafka-avro-sink.producer.type = async
    agent.sinks.kafka-avro-sink.serializer.class = kafka.serializer.DefaultEncoder
    agent.sinks.kafka-avro-sink.metadata.broker.list = 127.0.0.1:9092
    agent.sinks.kafka-avro-sink.avro.schema.file = /etc/flume/conf/schemas/testSchema.avsc
    agent.sinks.kafka-avro-sink.kafka.message.coder.schema.registry.class = com.linkedin.camus.schemaregistry.AvroRestSchemaRegistry
    agent.sinks.kafka-avro-sink.etl.schema.registry.url = http://127.0.0.1:2876/schema-repo
    agent.sinks.kafka-avro-sink.parser.class = org.redoop.flume.sink.avro.kafka.parsers.HelloWorldParser

Parsers
---------
You can implement any parser for each use case that receives a string line (Apache Log, Audit Log, Server Log, etc.). 

Example:

```java
public class HelloWorldParser implements Parser {

	public HashMap<String, Object> init(String line) {
		HashMap<String, Object> map = new HashMap<String, Object>();
		String fields[] = line.split(" ");
		map.put("Action", fields[0]);
		map.put("Message", fields[1]);
		return map;
	}

}
```

Related projects
---------

This project is inspired in [flume-ng-kafka-sink](https://github.com/baniuyao/flume-ng-kafka-sink) project. Thanks to [Frank Yao](https://github.com/baniuyao)

License
-------
Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0

--
Daniel Tardón <dtardon@redoop.org>
