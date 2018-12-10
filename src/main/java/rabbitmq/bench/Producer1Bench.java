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

package rabbitmq.bench;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.concurrent.TimeUnit;

/**
 * rabbitmq pubsub
 * 1 producer 2 consumer
 * 2kb   6853msg/s
 * 1kb  12400msg/s
 * 512b 26621msg/s
 * 256b 39238msg/s
 *
 * 1 producer 1 consumer
 * 2kb   7783msg/s
 * 1kb  14194msg/s
 * 512b 30743msg/s
 * 256b 49352msg/s
 *
 * 开启 HiPE
 * 1 producer 2 consumer
 * 2kb   8132msg/s
 * 1kb  12171msg/s
 * 512b 24182msg/s
 * 256b 38573msg/s
 *
 * 集群 pubsub
 * 1 producer 2 consumer
 * 2kb   8928msg/s
 * 1kb  14619msg/s
 * 512b 25319msg/s
 * 256b 40093msg/s
 *
 * @author Baoyi Chen
 */
public class Producer1Bench {

	private final static String EXCHANGE_NAME = "bench";

	private static byte[] MESSAGE = new byte[1024];

	static {
		for (int i = 0; i < MESSAGE.length; i++) {
			MESSAGE[i] = (byte) i;
		}
	}

	public static void main(String[] args) throws Exception {
		MetricRegistry registry = new MetricRegistry();
		Meter meter = registry.meter("rabbitmq.producer1.metric");
		ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).build();
		reporter.start(1, TimeUnit.SECONDS);

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("192.168.1.241");
		Connection connection = factory.newConnection();

		Channel channel = connection.createChannel();
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

		for (int i = 0; i < 2000000; i++) {
			try {
				channel.basicPublish(EXCHANGE_NAME, "", null, MESSAGE);
				meter.mark();
			} catch (Throwable e) {
			}
		}
		System.out.println(meter.getCount());
		channel.close();

		System.in.read();
		connection.close();
	}
}
