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

package rabbitmq.pubsub;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

/**
 * @author Baoyi Chen
 */
public class RabbitmqConsumer {

	private static final String EXCHANGE_NAME = "logs";

	public static void main(String[] args) throws Exception {
		worker();
		worker();
		System.in.read();
	}

	public static void worker() {
		new Thread(() -> {
			try {
				String thread = Thread.currentThread().toString();
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost("192.168.1.241");
				Connection connection = factory.newConnection();
				final Channel channel = connection.createChannel();

				channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
				String queueName = channel.queueDeclare().getQueue();
				channel.queueBind(queueName, EXCHANGE_NAME, "");

				System.out.println(thread + " [*] Waiting for messages. To exit press CTRL+C");

				Consumer consumer = new DefaultConsumer(channel) {
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
						String message = new String(body, "UTF-8");
						System.out.println(thread + " [x] Received '" + message + "'");

					}
				};
				channel.basicConsume(queueName, true, consumer);
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}).start();
	}
}
