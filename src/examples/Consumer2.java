/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package examples;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author 野狼
 * 2016年12月3日
 * TODO
 */
public class Consumer2  {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;

    /**
     * @param topic
     * 创建一个consumer，并且配置consumer所需要的参数。
     */
    public Consumer2(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
       // props.put(ConsumerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }
    
    public int test(){
    	consumer.subscribe(Collections.singletonList(this.topic));
    	ConsumerRecords<Integer, String> records = consumer.poll(9000);
    	 for (ConsumerRecord<Integer, String> record : records) {
         	//key可能对应是：partition的分区的，（测试一下），value代表消息内容。offset代表：parititon中的偏移量。
//         	partition：这个consumer从topic中已经获得了，
             System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
         }
    	System.out.println("--------------------------------"+records.count()+"-----------------------------");
    	System.out.println("你好啊");
    	return records.count();
    }


}
