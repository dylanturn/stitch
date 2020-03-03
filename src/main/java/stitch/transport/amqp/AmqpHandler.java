package stitch.transport.amqp;

import com.rabbitmq.client.*;
import org.apache.log4j.Logger;
import stitch.rpc.RpcRequest;
import stitch.rpc.RpcResponse;
import stitch.rpc.RpcRequestHandler;
import stitch.transport.TransportHandler;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.item.ConfigItemType;

import java.io.IOException;

public class AmqpHandler implements DeliverCallback {

    static final Logger logger = Logger.getLogger(AmqpHandler.class);

    private TransportHandler transportHandler;
    private Channel channel;
    private final Object monitor;
    private String exchange;

    public AmqpHandler(TransportHandler transportHandler, Channel channel, Object monitor, String exchange){
        this.transportHandler = transportHandler;
        this.channel = channel;
        this.monitor = monitor;
        this.exchange = exchange;
    }

    @Override
    public void handle(String consumerTag, Delivery delivery) throws IOException {

        logger.trace("Handling AMQP delivery!");
        AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                .Builder()
                .correlationId(delivery.getProperties().getCorrelationId())
                .build();

        // Get the RPC request from the amqp body. Route it, fulfill it, then get and return the response bytes.
        logger.trace("RPC Request received!");
        RpcRequest rpcRequest = RpcRequest.fromByteArray(delivery.getBody());
        logger.trace("RPC Response received!");
        RpcResponse rpcResponse = transportHandler.handleRequest(rpcRequest);
        logger.trace("RPC Response bytes received!");
        logger.trace("RPC Response Code: " + rpcResponse.getStatusCode());
        logger.trace("RPC Response Message: " + rpcResponse.getStatusMessage());
        byte[] rpcResponseBytes = RpcResponse.toByteArray(rpcResponse);
        logger.trace("RPC Response Length: " + rpcResponseBytes.length);

        // If this was a broadcast then there's nothing to return.
        if(delivery.getProperties().getReplyTo() != null){
            // Publish the reply to the caller and ack the message.
            logger.trace("Responding to: " + delivery.getProperties().getReplyTo());
            // We dont publish to an exchange because reply queues dont consume on them.
            channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, rpcResponseBytes);
        }

        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        synchronized (monitor) {
            monitor.notify();
        }
    }
}
