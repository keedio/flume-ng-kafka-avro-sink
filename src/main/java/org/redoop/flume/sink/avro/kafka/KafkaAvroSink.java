/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package org.redoop.flume.sink.avro.kafka;

import java.io.File;
import java.util.HashMap;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.apache.avro.generic.GenericData.Record;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Sink of Kafka which get events from channels and publish to Kafka Serialized in Avro.
 * <tt>zk.connect: </tt> the zookeeper ip kafka use.
 * <p>
 * <tt>topic: </tt> the topic to read from kafka.
 * <p>
 * <tt>batchSize: </tt> send serveral messages in one request to kafka.
 * <p>
 * <tt>producer.type: </tt> type of producer of kafka, async or sync is
 * available.<o> <tt>serializer.class: </tt>{@kafka.serializer.StringEncoder
 * 
 * 
 * }
 */
public class KafkaAvroSink extends AbstractSink implements Configurable {
	private static final Logger log = LoggerFactory.getLogger(KafkaAvroSink.class);
	private String topic;
	private Producer<byte[], byte[]> producer;
	private File avroSchemaFile;
	private Properties props;
	

	public Status process() throws EventDeliveryException {
		Channel channel = getChannel();
		Transaction tx = channel.getTransaction();
		try {
			tx.begin();
			Event event = channel.take();
			if (event == null) {
				tx.commit();
				return Status.READY;

			}
			
			String line = new String (event.getBody());
			HashMap<String, Object> map = KafkaAvroSinkUtil.parseMessage(props,line);
			Record record = KafkaAvroSinkUtil.fillRecord(KafkaAvroSinkUtil.fillAvroTestSchema(avroSchemaFile),map);
			byte[] avroRecord = KafkaAvroSinkUtil.encodeMessage(topic,record,props);
			
	        producer.send(new KeyedMessage<byte[], byte[]>(this.topic, avroRecord));

			tx.commit();
			return Status.READY;
		} catch (Exception e) {
			try {
				tx.rollback();
				return Status.BACKOFF;
			} catch (Exception e2) {
				log.error("Rollback Exception:{}", e2);
			}		
			log.error("KafkaAvroSink Exception:{}", e);
			return Status.BACKOFF;
		} finally {
			tx.close();
		}
	}

	public void configure(Context context) {
		topic = context.getString("topic");
		if (topic == null) {
			throw new ConfigurationException("Kafka topic must be specified.");
		}
		// Get schema file
		String avroSchemaFileName = context.getString("avro.schema.file");
		if (avroSchemaFileName == null) {
			throw new ConfigurationException("Avro schema must be specified.");
		}else{
			avroSchemaFile = new File(avroSchemaFileName);
		}
		producer = KafkaAvroSinkUtil.getProducer(context);
		props = KafkaAvroSinkUtil.getKafkaConfigProperties(context);
	}

	@Override
	public synchronized void start() {
		super.start();
	}

	@Override
	public synchronized void stop() {
		producer.close();
		super.stop();
	}
}
