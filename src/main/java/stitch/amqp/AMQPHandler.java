package stitch.amqp;

import com.rabbitmq.client.*;
import org.apache.log4j.Logger;
import stitch.amqp.rpc.RPCRecord;
import stitch.amqp.rpc.RPCStats;
import stitch.amqp.rpc.RPCStatusCode;

import java.io.IOException;

public abstract class AMQPHandler implements DeliverCallback {

    static final Logger logger = Logger.getLogger(AMQPHandler.class);

    private AMQPServer amqpServer;

    public AMQPHandler(AMQPServer amqpServer){
        this.amqpServer = amqpServer;
    }

    @Override
    public void handle(String consumerTag, Delivery delivery) throws IOException {
        AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                .Builder()
                .correlationId(delivery.getProperties().getCorrelationId())
                .build();

        Channel channel = amqpServer.getChannel();
        Object monitor = amqpServer.getMonitor();
        String exchange = amqpServer.getExchange();

        logger.info("RPC Request received!");
        // Get the message headers and body.
        AMQP.BasicProperties messageProperties = delivery.getProperties();
        byte[] messageBytes = delivery.getBody();

        // Create the RPC record used to track the RPC performance.
        logger.info("RPC Type: " + messageProperties.getType());
        LongString longCallerId = (LongString)messageProperties.getHeaders().get("caller_id");
        RPCRecord rpcRecord = amqpServer.startRPC(longCallerId.toString(), delivery.getProperties().getType());

        byte[] responseBytes = null;

        try {
            // Check to see if the RPC call should be routed internally before
            responseBytes = routeInternalRPC(delivery.getProperties(), messageBytes);
            if(responseBytes == null){
                responseBytes = routeRPC(delivery.getProperties(), messageBytes);
            }

        } catch (RuntimeException error) {
            /* End the RPC Call with the status code ERROR. */
            logger.error("Failed to handle RPC call!", error);
            rpcRecord.endCall(RPCStatusCode.ERROR);

        } finally {
            // Publish the reply to the caller and ack the message.
            channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, responseBytes);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            /* End the RPC Call with the status code OK. */
            rpcRecord.endCall(RPCStatusCode.OK);
            synchronized (monitor) {
                // Honestly not sure why I'm expected to do this, seem like a locking thing?
                // Could probably figure it out but it's what's documented and it works, so meh (for now, I promise)
                monitor.notify();
            }
        }
    }


    protected abstract byte[] routeRPC(AMQP.BasicProperties messageProperties, byte[] messageBytes);


    private byte[] routeInternalRPC(AMQP.BasicProperties messageProperties, byte[] messageBytes){
        switch (messageProperties.getType()) {
            case "RPC_getRPCStats":
                try {
                    return RPCStats.toByteArray(amqpServer.getRpcStats());
                } catch (IOException error) {
                    logger.error("Failed to get RPC stats!", error);
                    return null;
                }

            default:
                return null;
        }
    }


}
