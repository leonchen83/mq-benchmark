package rabbitmq.fc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BlockedListener;
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
public class FlowControlDemo {
    
    private static byte[] MESSAGE = new byte[1024];
    
    static {
        for (int i = 0; i < MESSAGE.length; i++) {
            MESSAGE[i] = (byte) i;
        }
    }
    
    private static final String EXCHANGE = "flow-control";
    
    public static void main(String[] args) {
        consume();
        produce();
    }
    
    public static void consume() {
        new Thread(() -> {
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("192.168.1.242");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, EXCHANGE, "");
    
                Consumer consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                };
                channel.basicConsume(queueName, true, consumer);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }).start();
    }
    
    public static void produce() {
        new Thread(() -> {
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("192.168.1.242");
                Connection connection = factory.newConnection();
                connection.addBlockedListener(new BlockedListener() {
                    @Override
                    public void handleBlocked(String reason) throws IOException {
                        System.out.println("blocked:" + reason);
                    }
                    
                    @Override
                    public void handleUnblocked() throws IOException {
                        System.out.println("unblocked");
                    }
                });
                Channel channel = connection.createChannel();
                channel.exchangeDeclare(EXCHANGE, "fanout");
                
                for (int i = 0; i < 1000000; i++) {
                    try {
                        channel.basicPublish(EXCHANGE, "", null, MESSAGE);
                    } catch (Throwable e) {
                    }
                }
                channel.close();
                connection.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }).start();
    }
    
}
