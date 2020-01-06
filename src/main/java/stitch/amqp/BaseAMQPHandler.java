package stitch.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.apache.log4j.Logger;

import java.io.IOException;

public abstract class BaseAMQPHandler implements DeliverCallback {

    static final Logger logger = Logger.getLogger(BaseAMQPHandler.class);

    private BasicAMQPServer basicAMQPServer;

    public BaseAMQPHandler(BasicAMQPServer basicAMQPServer){
        this.basicAMQPServer = basicAMQPServer;
    }

    @Override
    public void handle(String consumerTag, Delivery delivery) throws IOException {
        AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                .Builder()
                .correlationId(delivery.getProperties().getCorrelationId())
                .build();

        // Incase we dont have a response I guess.....
        byte[] responseBytes = new byte[0];

        Channel channel = basicAMQPServer.getChannel();
        Object monitor = basicAMQPServer.getMonitor();
        String exchange = basicAMQPServer.getExchange();

        logger.info("RPC Request received!");
        try {
            logger.info("RPC Type: " + delivery.getProperties().getType());
            responseBytes = routeRPC(delivery.getProperties(), delivery.getBody());
            logger.info("Response recieved!!");
        } catch (RuntimeException e) {
            System.out.println(" [.] " + e.toString());
        } finally {
            channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, responseBytes);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            synchronized (monitor) {
                monitor.notify();
            }
        }
    }
    protected abstract byte[] routeRPC(AMQP.BasicProperties messageProperties, byte[] messageBytes);
}
