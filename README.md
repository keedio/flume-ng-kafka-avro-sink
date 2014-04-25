flume-ng-kafka-avro-sink
================

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with [kafka 0.8.0](http://kafka.apache.org/08/quickstart.html) sending Avro messages to the Kafka cluster.

Configuration of Kafka Avro Sink
----------

    agent_log.sinks.kafka.type = com.vipshop.flume.sink.kafka.KafkaSink
    agent_log.sinks.kafka.channel = all_channel
    agent_log.sinks.kafka.zk.connect = 127.0.0.1:2181
    agent_log.sinks.kafka.topic = all
    agent_log.sinks.kafka.batchsize = 200
    agent_log.sinks.kafka.producer.type = async
    agent_log.sinks.kafka.serializer.class = kafka.serializer.StringEncoder

Related projects
---------

This project is inspired in [flume-ng-kafka-sink](https://github.com/baniuyao/flume-ng-kafka-sink) project. Thanks to [Baniuyao](https://github.com/baniuyao).


