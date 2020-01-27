package stitch.rpc.transport.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.apache.log4j.Logger;
import stitch.rpc.transport.RpcCallableAbstract;
import stitch.rpc.transport.RpcRequestHandler;
import stitch.rpc.transport.RpcCallableServer;
import stitch.util.configuration.item.ConfigItem;

import java.io.IOException;
import java.net.URI;

public class AMQPServer extends RpcCallableAbstract implements RpcCallableServer {

    private static final Logger logger = Logger.getLogger(AMQPServer.class);

    private String exchange;
    private Connection connection;
    private Channel channel;
    private final Object monitor;

    private DeliverCallback deliverCallback;

    public AMQPServer(ConfigItem endpointConfig, RpcRequestHandler rpcRequestHandler) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        super(endpointConfig);
        exchange = transportConfig.getConfigString("exchange");
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

            logger.trace("Attaching AMQP delivery callback");
            this.deliverCallback = new AMQPHandler(rpcRequestHandler, channel, monitor, exchange);

            logger.debug("AMQP connected!");
            logger.trace(String.format("Declaring AMQP queue:             %s", getRpcAddress()));
            channel.queueDeclare(getRpcAddress(), false, false, false, null);
            logger.trace(String.format("Binding to AMQP queue:            %s", getRpcAddress()));
            channel.queueBind(getRpcAddress(), exchange, getRpcAddress());
        } catch (Exception error) {
            logger.error("Failed to start the AMQP listener!", error);
        }

        try {
            logger.trace(String.format("Listening for AMQP messages from: %s", hostname));
            channel.basicConsume(getRpcAddress(), false, deliverCallback, (consumerTag -> {
            }));
        } catch (IOException error) {
            logger.error(String.format("Failed to consume channel: %s", getRpcAddress()));
        }

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

    @Override
    public boolean isReady(){
        if(channel != null && channel.isOpen())
            return true;
        return false;
    }

    @Override
    public boolean isConnected(){
        if(connection != null && connection.isOpen())
            return true;
        return false;
    }
}
