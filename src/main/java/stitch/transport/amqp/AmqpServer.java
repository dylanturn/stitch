package stitch.transport.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.apache.log4j.Logger;
import stitch.transport.Transport;
import stitch.transport.TransportCallableServer;
import stitch.transport.TransportHandler;
import stitch.util.configuration.item.ConfigItem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class AmqpServer extends Transport implements TransportCallableServer {

    private static final Logger logger = Logger.getLogger(AmqpServer.class);

    private Connection connection;
    private Channel channel;
    private final Object monitor;

    private String amqpServerType;
    private String baseExchange;
    private String directExchange;
    private String broadcastExchange;
    private DeliverCallback deliverCallback;
    private DeliverCallback broadcastCallback;

    public AmqpServer(ConfigItem endpointConfig, TransportHandler transportHandler) throws IOException, IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException, TimeoutException {
        super(endpointConfig);

        baseExchange = transportConfig.getConfigString("exchange");
        amqpServerType = endpointConfig.getConfigType().toString();
        directExchange = String.format("%s.%s.direct", baseExchange, amqpServerType);
        broadcastExchange = String.format("%s.%s.broadcast", baseExchange, amqpServerType);

        logger.info("Starting up AMQP server...");
        logger.info(String.format("Type: %s", transportConfig.getConfigType().toString()));
        logger.info(String.format("Id:     %s", transportConfig.getConfigId()));

        String hostname = transportConfig.getConfigString("host");
        String username = transportConfig.getConfigString("username");
        String password = transportConfig.getConfigString("password");

        this.monitor = new Object();


        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI.create(String.format("amqp://%s:%s@%s/%s", username, password, hostname, username)));
        connection = factory.newConnection();
        channel = connection.createChannel();

        // We do this first because we'll be needing to handle messages as soon as bind to something.
        logger.trace("Attaching AMQP delivery callback");
        this.deliverCallback = new AmqpHandler(transportHandler, channel, monitor, directExchange);
        this.broadcastCallback = new AmqpHandler(transportHandler, channel, monitor, broadcastExchange);

        // Make sure the AMQP exchanges exist.
        declareExchanges();

        // Make sure the AMQP queues exist.
        declareQueues();

        // Bind the AMQP queues
        bindQueues();
    }

    private void declareExchanges() throws IOException {
        // Declare the directExchange for this endpoint
        logger.trace(String.format("Declaring AMQP Direct Exchange:             %s", directExchange));
        channel.exchangeDeclare(directExchange, "direct", true);

        // Make sure the aggregators broadcast channel exists
        logger.trace(String.format("Declaring AMQP Fanout Exchange:             %s", broadcastExchange));
        channel.exchangeDeclare(broadcastExchange, "fanout", true);
    }

    private void declareQueues() throws IOException {

        // Get the queue arguments.
        Map<String, Object> channelArgs = transportConfig.getObjectMap("channel_args");
        Map<String, Object> unicastArgs = (Map<String, Object>)channelArgs.get("unicast");

        // Now we declare the queue to make sure it exists. (These declare operations are meant to be idempotent)
        logger.trace(String.format("Declaring AMQP direct queue:             %s", getRpcAddress()));
        channel.queueDeclare(getRpcAddress(), false, false, false, unicastArgs);
    }

    private void bindQueues() throws IOException {
        // At this point we bind to the queue
        logger.trace(String.format("Binding to AMQP Direct queue:            %s", getRpcAddress()));
        channel.queueBind(getRpcAddress(), directExchange, getRpcAddress());

        logger.trace(String.format("Binding to AMQP Fanout queue:            %s", getRpcAddress()));
        channel.queueBind(getRpcAddress(), broadcastExchange, getRpcAddress());
    }

    private void consumeQueues() throws IOException {
        logger.trace(String.format("Listening for unicast AMQP messages from: %s", transportConfig.getConfigString("host")));
        channel.basicConsume(getRpcAddress(), false, deliverCallback, (consumerTag -> {}));

        logger.trace(String.format("Listening for broadcast AMQP messages from: %s", transportConfig.getConfigString("host")));
        channel.basicConsume(getRpcAddress(), false, broadcastCallback, (consumerTag -> {}));
    }

    @Override
    public void run() {

        // Begin consuming messages from the AMQP queues.
        try {
            consumeQueues();
            logger.debug("AMQP connected and running!");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Runnable runnable = () -> {
            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized (monitor) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        Thread t = new Thread(runnable);
        t.start();
    }
}
