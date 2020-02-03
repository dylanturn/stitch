package stitch.transport.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.log4j.Logger;
import stitch.rpc.RpcRequest;
import stitch.rpc.RpcResponse;
import stitch.transport.Transport;
import stitch.transport.TransportCallableClient;
import stitch.util.configuration.item.ConfigItem;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class AmqpClient extends Transport implements TransportCallableClient {

    private static final Logger logger = Logger.getLogger(AmqpClient.class);

    private String exchange;
    private Connection connection;
    private Channel channel;
    private Object monitor;

    public AmqpClient(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        super(endpointConfig);

        exchange = transportConfig.getConfigString("exchange");
        String username = transportConfig.getConfigString("username");
        String password = transportConfig.getConfigString("password");
        String hostname = transportConfig.getConfigString("host");

        this.monitor = new Object();

        // Connect to the AMQP endpoints
        try{
            ConnectionFactory factory = new ConnectionFactory();
            URI amqpURI = URI.create(String.format("amqp://%s:%s@%s/%s", username, password, hostname, username));
            factory.setUri(amqpURI);
            connection = factory.newConnection();

            channel = connection.createChannel();

            // Declare the exchange for this endpoint
            channel.exchangeDeclare(exchange, "direct");

            // Make sure the aggregators broadcast channel exists
            channel.exchangeDeclare(String.format("%s_broadcast", exchange), "fanout");

            logger.debug("AMQP connected!");
        } catch(Exception error){
            logger.error(String.format("Failed to connect to the AMQP host: ",hostname), error);
        }
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

    @Override
    public RpcResponse invokeRPC(RpcRequest rpcRequest) throws IOException, InterruptedException {

        logger.trace("Invoke RPC");

        // Setup the queue and correlation id that the server will use to reply to the client.
        final String corrId = UUID.randomUUID().toString();
        String replyQueueName = channel.queueDeclare().getQueue();

        // Create the configuration that will be sent with the RPC call.
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        logger.info("Queue:       " + rpcRequest.getDestination());
        logger.info("CorrID:      " + corrId);
        logger.info("Reply Queue: " + replyQueueName);
        logger.info("Method:      " + rpcRequest.getMethod());

        logger.debug("Publishing the creation RPC...");
        // Publish the RPC call to the queue.
        rpcRequest.setSource(corrId);
        channel.queueDeclare(rpcRequest.getDestination(), false, false, false, null);
        channel.basicPublish(exchange, rpcRequest.getDestination(), props, RpcRequest.toByteArray(rpcRequest));
        final BlockingQueue<Object> response = new ArrayBlockingQueue<>(1);

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                try {
                    response.offer(RpcResponse.fromByteArray(delivery.getBody()));
                } catch( ClassNotFoundException error) {
                    logger.error("Failed to load RPC response from bytes", error);
                }
            }
        }, consumerTag -> {
        });

        logger.debug("Waiting for response...");
        RpcResponse rpcResponse = (RpcResponse)response.take();
        channel.basicCancel(ctag);
        return rpcResponse;
    }

    public RpcResponse invokeBroadcast(RpcRequest rpcRequest) throws IOException, InterruptedException {
        return null;
    }

    public RpcResponse invokeMulticast(RpcRequest rpcRequest) throws IOException, InterruptedException {
        return null;
    }
}
