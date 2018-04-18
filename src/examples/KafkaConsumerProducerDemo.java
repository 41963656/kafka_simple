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

public class KafkaConsumerProducerDemo {
	public static void main(String[] args) {
		System.out.println(test());
	}

	public static int test() {
		System.out.println("start");
		Producer producerThread = new Producer(KafkaProperties.TOPIC, false);
		producerThread.start();
		try {
			producerThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("end");
		 Consumer consumerThread = new Consumer(KafkaProperties.TOPIC);
		 consumerThread.start();
		 Consumer2 consumer2 = new Consumer2(KafkaProperties.TOPIC);
		return 0;
	}
}
