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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Producer extends Thread {
	private final KafkaProducer<String, String> producer;
	private final String topic;
	private final Boolean isAsync;
	public static Map<Integer, Integer> resultMap = new HashMap<Integer, Integer>();
	private int messageNo;

	/**
	 * @param topic
	 *            producer需要往哪个topic中传送数据。
	 * @param isAsync
	 *            是否异步 构造函数，配置producer必要的信息，创建producer。
	 */
	public Producer(String topic, Boolean isAsync) {
		Properties props = new Properties();
		props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
		props.put("client.id", "DemoProducer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("partitioner.class", "examples.SimplePartitioner");
		producer = new KafkaProducer<>(props);
		this.topic = topic;
		this.isAsync = isAsync;
	}

	// 线程必须重写的方法
	public void run() {
		messageNo = 1;
		// 我只让他执行100次，停止。
		while (messageNo < 50) {
			String messageStr = "message" + messageNo;
			long startTime = System.currentTimeMillis();
			if (isAsync) { // Send asynchronously
				// producerRecord这个类是一个消息，参数是：topic，key(用来存储在哪个partition上的)，value(消息体)
				// callback接口，为了
				ProducerRecord<String, String> mess = new ProducerRecord<>(topic, messageStr, messageStr);
				producer.send(mess, new Callback() {
					public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
						if (recordMetadata != null) {
							System.out.println("message" + messageNo + " sent to partition("
									+ recordMetadata.partition() + "), " + "offset(" + recordMetadata.offset() + ")");
						} else {
							exception.printStackTrace();
						}
					}
				});
				// producer.send(new ProducerRecord<>(topic, messageNo,
				// messageStr),
				// new DemoCallBack(startTime, messageNo, messageStr));
			} else { // Send synchronously

				try {
					// 同步模式，调用get方法是因为等待这个方法执行完毕，然后方法会返回一个计算结果。
					// RecordMetadata 这个类 记录的是返回的结果（包括topic，partition，offset等），
					// recordMetadata 是和异步模式中的调用的callback类onCompletion方法中的参数一样
					RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic, messageStr, messageStr))
							.get();
					System.out.println("recordMetaData.offset===" + recordMetadata.offset());
					System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
			++messageNo;
			if (messageNo % 100 == 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

class DemoCallBack implements Callback {

	private final long startTime;
	private final int key;
	private final String message;

	/**
	 * @param startTime
	 * @param key
	 *            key
	 * @param message
	 *            消息体
	 * 
	 */
	public DemoCallBack(long startTime, int key, String message) {
		this.startTime = startTime;
		this.key = key;
		this.message = message;
	}

	/**
	 * A callback method the user can implement to provide asynchronous handling
	 * of request completion. This method will be called when the record sent to
	 * the server has been acknowledged. Exactly one of the arguments will be
	 * non-null.
	 *
	 * @param metadata
	 *            The metadata for the record that was sent (i.e. the partition
	 *            and offset). Null if an error occurred.
	 * @param exception
	 *            The exception thrown during processing of this record. Null if
	 *            no error occurred.
	 */
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		if (metadata != null) {
			System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), "
					+ "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
		} else {
			exception.printStackTrace();
		}
	}
}
