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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flume.Context;
import org.redoop.flume.sink.avro.kafka.parsers.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageEncoder;


public class KafkaAvroSinkUtil {
	private static final Logger log = LoggerFactory.getLogger(KafkaAvroSinkUtil.class);
	public static final String PARSER_CLASS = "parser.class";

	public static Properties getKafkaConfigProperties(Context context) {
		log.info("context={}",context.toString());
		Properties props = new Properties();
		Map<String, String> contextMap = context.getParameters();
		for(String key : contextMap.keySet()) {
			if (!key.equals("type") && !key.equals("channel")) {
				props.setProperty(key, context.getString(key));
				log.info("key={},value={}",key,context.getString(key));
			}
		}
		return props;
	}
	public static Producer<byte[], byte[]> getProducer(Context context) {
		Producer<byte[], byte[]> producer;
		producer = new Producer<byte[], byte[]>(new ProducerConfig(getKafkaConfigProperties(context)));
		return producer;
	}
	
	public static byte[] encodeMessage(String topic, IndexedRecord record, Properties props){
    	KafkaAvroMessageEncoder encoder = new KafkaAvroMessageEncoder(topic, null);
		encoder.init(props, topic);
    	return encoder.toBytes(record);
    }
	
	@SuppressWarnings("deprecation")
	public static Schema fillAvroTestSchema(File jsonSchemaFile) throws IOException{
     	//Schema.Parser schemaParser = Schema.Parser();
    	return Schema.parse(jsonSchemaFile);
    }
    
    public static Record fillRecord(Schema schema, HashMap<String, Object> map){
		Record record = new Record(schema);
		for ( String key : map.keySet() ) {
		    record.put(key, map.get(key));
		}		    	
    	return record;
    }
	public static HashMap<String, Object> parseMessage(String line) {
		HashMap<String, Object> map = new HashMap<String, Object>();
		String fields[] = line.split(" ");
		map.put("Action", fields[0]);
		map.put("Message", fields[1]);
		return map;
	}
	
	public static HashMap<String, Object> parseMessage(Properties props, String line) {
		try {
			Parser parser = (Parser) Class.forName(props.getProperty(PARSER_CLASS)).newInstance();
			return parser.init(line);			
		} catch (Exception e) {
			log.error("KafkaAvroUtilSink Exception:{}", e);
			return null;
		}
		
	}
	
	
}


















