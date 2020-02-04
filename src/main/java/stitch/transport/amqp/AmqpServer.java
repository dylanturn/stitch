package stitch.transport.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.apache.log4j.Logger;
import stitch.transport.Transport;
import stitch.rpc.RpcRequestHandler;
import stitch.transport.TransportCallableServer;
import stitch.transport.TransportHandler;
import stitch.util.configuration.item.ConfigItem;

import java.io.IOException;
import java.net.URI;

    public class AmqpServer extends Transport implements TransportCallableServer {

    private static final Logger logger = Logger.getLogger(AmqpServer.class);

    private String directExchange;
    private String broadcastExchange;
    private Connection connection;
    private Channel channel;
    private final Object monitor;

    private DeliverCallback deliverCallback;

    public AmqpServer(ConfigItem endpointConfig, TransportHandler transportHandler) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        super(endpointConfig);

        directExchange = transportConfig.getConfigString("exchange");
        broadcastExchange = String.format("%s.broadcast", directExchange);

        logger.info("Starting up AMQP server...");
        logger.info(String.format("Type: %s", transportConfig.getConfigType().toString()));
        logger.info(String.format("Id:     %s", transportConfig.getConfigId()));

        String hostname = transportConfig.getConfigString("host");
        String username = transportConfig.getConfigString("username");
        String password = transportConfig.getConfigString("password");

        this.monitor = new Object();

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(URI.create(String.format("amqp://%s:%s@%s/%s", username, password, hostname, username)));
            connection = factory.newConnection();
            channel = connection.createChannel();

            // We do this first because we'll be needing to handle messages as soon as bind to something.
            logger.trace("Attaching AMQP delivery callback");
            this.deliverCallback = new AmqpHandler(transportHandler, channel, monitor, directExchange);

            // Do what we need to do to make sure out AMQP exchanges exist.
            declareExchanges();

            logger.debug("AMQP connected!");

            // Do what needs to be done to declare and bind to the queue that sends regular RPC requests
            declareAndBindRpc();

        } catch (Exception error) {
            logger.error("Failed to start the AMQP listener!", error);
        }

        try {
            logger.trace(String.format("Listening for AMQP messages from: %s", hostname));
            channel.basicConsume(getRpcAddress(), false, deliverCallback, (consumerTag -> {}));
        } catch (IOException error) {
            logger.error(String.format("Failed to consume channel: %s", getRpcAddress()));
        }

    }
    private void declareExchanges() throws IOException {
        // Declare the directExchange for this endpoint
        logger.trace(String.format("Declaring AMQP Direct Exchange:             %s", directExchange));
        channel.exchangeDeclare(directExchange, "direct", true);

        // Make sure the aggregators broadcast channel exists
        logger.trace(String.format("Declaring AMQP Fanout Exchange:             %s", broadcastExchange));
        channel.exchangeDeclare(broadcastExchange, "fanout", true);
    }

    private void declareAndBindBroadcast() throws IOException {
        channel.queueBind(getRpcAddress(), broadcastExchange, getRpcAddress());
    }

    private void declareAndBindRpc() throws IOException {
        // Now we declare the queue to make sure it exists. (These declare operations are meant to be idempotent)
        logger.trace(String.format("Declaring AMQP queue:             %s", getRpcAddress()));
        channel.queueDeclare(getRpcAddress(), false, false, false, null);

        // At this point we bind to the queue
        logger.trace(String.format("Binding to AMQP queue:            %s", getRpcAddress()));
        channel.queueBind(getRpcAddress(), directExchange, getRpcAddress());
    }

    @Override
    public void run() {
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
