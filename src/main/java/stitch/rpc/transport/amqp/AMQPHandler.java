package stitch.rpc.transport.amqp;

import com.rabbitmq.client.*;
import org.apache.log4j.Logger;
import stitch.rpc.RPCRequest;
import stitch.rpc.RPCResponse;
import stitch.rpc.transport.RpcRequestHandler;

import java.io.IOException;

public class AMQPHandler implements DeliverCallback {

    static final Logger logger = Logger.getLogger(AMQPHandler.class);

    private RpcRequestHandler rpcRequestHandler;
    private Channel channel;
    private Object monitor;
    private String exchange;

    public AMQPHandler(RpcRequestHandler rpcRequestHandler, Channel channel, Object monitor, String exchange){
        this.rpcRequestHandler = rpcRequestHandler;
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
        RPCRequest rpcRequest = RPCRequest.fromByteArray(delivery.getBody());
        logger.trace("RPC Response received!");
        RPCResponse rpcResponse = rpcRequestHandler.handleRequest(rpcRequest);
        logger.trace("RPC Response bytes received!");
        logger.trace("RPC Response Code: " + rpcResponse.getStatusCode());
        logger.trace("RPC Response Message: " + rpcResponse.getStatusMessage());
        byte[] rpcResponseBytes = RPCResponse.toByteArray(rpcResponse);
        logger.trace("RPC Response Length: " + rpcResponseBytes.length);
        logger.trace("Responding to: " + delivery.getProperties().getReplyTo());
        // Publish the reply to the caller and ack the message.
        logger.trace("RPC Channel Open: " + channel.isOpen());
        channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, rpcResponseBytes);
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        synchronized (monitor) {
            monitor.notify();
        }
    }
}
