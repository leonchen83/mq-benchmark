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

package activemq.bench;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import java.util.concurrent.TimeUnit;

/**
 * @author Baoyi Chen
 */
public class Consumer2Bench {

	private final static String EXCHANGE_NAME = "bench";

	public static void main(String[] args) throws Exception {

		MetricRegistry registry = new MetricRegistry();
		Meter meter = registry.meter("activemq.consumer2.metric");
		ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).build();
		reporter.start(1, TimeUnit.SECONDS);

		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("nio://192.168.1.241:61617");
		factory.setOptimizeAcknowledge(false);
		factory.setAlwaysSessionAsync(false);
		Connection connection = factory.createConnection();
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = session.createTopic(EXCHANGE_NAME);
		MessageConsumer consumer = session.createConsumer(dest);
		consumer.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message message) {
				if (message instanceof BytesMessage) {
					meter.mark();
				}
			}
		});
		System.in.read();
	}
}
