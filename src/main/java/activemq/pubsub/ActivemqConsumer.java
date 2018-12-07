/*
 * Copyright 2016-2017 Leon Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package activemq.pubsub;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

/**
 * @author Baoyi Chen
 */
public class ActivemqConsumer {

	public static void main(String[] args) throws Exception {
		worker("topic");
		worker("topic");
		System.in.read();
	}

	public static void worker(String topic) {
		new Thread(() -> {
			try {
				String thread = Thread.currentThread().toString();
				ConnectionFactory factory = new ActiveMQConnectionFactory("nio://192.168.1.241:61617");
				Connection connection = factory.createConnection();
				connection.start();
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				Destination dest = session.createTopic(topic);
				MessageConsumer consumer = session.createConsumer(dest);
				consumer.setMessageListener(new MessageListener() {
					@Override
					public void onMessage(Message message) {
						if (message instanceof BytesMessage) {
							BytesMessage bmsg = (BytesMessage) message;
							try {
								int len = bmsg.readInt();
								byte[] b = new byte[len];
								bmsg.readBytes(b);
								System.out.println(thread + ", receive : " + new String(b));
							} catch (JMSException e) {
								e.printStackTrace();
							}
						}
					}
				});
			} catch (Throwable e) {
				e.printStackTrace();
			}

		}).start();
	}
}
