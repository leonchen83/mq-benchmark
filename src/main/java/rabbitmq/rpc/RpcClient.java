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

package rabbitmq.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * @author Baoyi Chen
 */
public class RpcClient {
	private Connection connection;
	private Channel channel;
	private String requestQueueName = "rpc_queue";

	public RpcClient() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("192.168.1.241");

		connection = factory.newConnection();
		channel = connection.createChannel();
	}

	public String call(String message) throws IOException, InterruptedException {
		final String corrId = UUID.randomUUID().toString();

		String replyQueueName = channel.queueDeclare().getQueue();
		AMQP.BasicProperties props = new AMQP.BasicProperties
				.Builder()
				.correlationId(corrId)
				.replyTo(replyQueueName)
				.build();

		channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

		final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);

		String ctag = channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				if (properties.getCorrelationId().equals(corrId)) {
					response.offer(new String(body, "UTF-8"));
				}
			}
		});

		String result = response.take();
		channel.basicCancel(ctag);
		return result;
	}

	public void close() throws IOException {
		connection.close();
	}

	public static void main(String[] argv) {
		RpcClient fibonacciRpc = null;
		String response = null;
		try {
			fibonacciRpc = new RpcClient();

			for (int i = 0; i < 32; i++) {
				String i_str = Integer.toString(i);
				System.out.println(" [x] Requesting fib(" + i_str + ")");
				response = fibonacciRpc.call(i_str);
				System.out.println(" [.] Got '" + response + "'");
			}
		} catch (IOException | TimeoutException | InterruptedException e) {
			e.printStackTrace();
		} finally {
			if (fibonacciRpc != null) {
				try {
					fibonacciRpc.close();
				} catch (IOException _ignore) {
				}
			}
		}
	}

}
