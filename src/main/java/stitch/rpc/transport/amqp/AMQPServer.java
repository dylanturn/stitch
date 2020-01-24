package stitch.rpc.transport.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.apache.log4j.Logger;
import stitch.rpc.transport.RpcRequestHandler;
import stitch.rpc.transport.RpcCallableServer;
import stitch.util.properties.StitchProperty;

import java.io.IOException;
import java.net.URI;

public class AMQPServer implements RpcCallableServer {

    private static final Logger logger = Logger.getLogger(AMQPServer.class);

    private String parentType;
    private String parentId;
    private String parentRoute;
    private StitchProperty transportProperties;

    private String exchange;
    private Connection connection;
    private Channel channel;
    private Object monitor;

    private DeliverCallback deliverCallback;

    @Override
    public RpcCallableServer setProperties(StitchProperty transportProperties) {
        this.transportProperties = transportProperties;
        exchange = transportProperties.getPropertyString("exchange");
        return this;
    }

    @Override
    public RpcCallableServer setParent(String parentType, String parentId){
        this.parentType = parentType;
        this.parentId = parentId;
        this.parentRoute = String.format("%s_%s", parentType, parentType);
        return this;
    }

    @Override
    public RpcCallableServer setRpcHandler(RpcRequestHandler rpcRequestHandler) {
        logger.info("Attaching AMQP delivery callback");
        this.deliverCallback = new AMQPHandler(rpcRequestHandler, channel, monitor, exchange);
        return this;
    }

    @Override
    public void run() {

        String username = transportProperties.getPropertyString("username");
        String password = transportProperties.getPropertyString("password");
        String hostname = transportProperties.getPropertyString("host");

        logger.info("Starting up AMQP server...");
        logger.info(String.format("Type: %s", parentType));
        logger.info(String.format("Id:     %s", parentId));

        this.monitor = new Object();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            URI amqpURI = URI.create(String.format("amqp://%s:%s@%s/%s", username, password, hostname, username));
            factory.setUri(amqpURI);
            connection = factory.newConnection();
            channel = connection.createChannel();
            logger.debug("AMQP connected!");
            logger.info(String.format("Declaring AMQP queue:             %s", parentRoute));
            channel.queueDeclare(parentRoute, false, false, false, null);
            logger.info(String.format("Binding to AMQP queue:            %s", parentRoute));
            channel.queueBind(parentRoute, exchange, parentRoute);
        } catch (Exception error) {
            logger.error("Failed to start the AMQP listener!", error);
        }

        try {
            logger.info(String.format("Listening for AMQP messages from: %s", hostname));
            channel.basicConsume(parentRoute, false, deliverCallback, (consumerTag -> {
            }));
        } catch (IOException error) {
            logger.error(String.format("Failed to consume channel: %s", parentRoute));
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
