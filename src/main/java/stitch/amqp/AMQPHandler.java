package stitch.amqp;

import com.rabbitmq.client.*;
import org.apache.log4j.Logger;
import stitch.amqp.rpc.RPCRecord;
import stitch.amqp.rpc.RPCRequest;
import stitch.amqp.rpc.RPCResponse;
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

        // Get the RPC request from the amqp body. Route it, fulfill it, then get and return the response bytes.
        logger.trace("RPC Request received!");
        RPCRequest rpcRequest = RPCRequest.fromByteArray(delivery.getBody());
        byte[] rpcResponseBytes = RPCResponse.toByteArray(routeInternalRPC(rpcRequest));

        // Publish the reply to the caller and ack the message.
        channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, rpcResponseBytes);
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        synchronized (monitor) {
            // Honestly not sure why I'm expected to do this, seem like a locking thing?
            // Could probably figure it out but it's what's documented and it works, so meh (for now, I promise)
            monitor.notify();
        }
    }

    protected abstract RPCResponse routeRPC(RPCRequest rpcRequest);

    private RPCResponse routeInternalRPC(RPCRequest rpcRequest){
        switch (rpcRequest.getMethod()) {

            case "getRPCStats":
                try {
                    return rpcRequest.createResponse()
                            .setStatusCode(RPCStatusCode.OK)
                            .setResponseObject(amqpServer.getAmqpStats());
                } catch (IOException error) {
                    logger.error("Failed to get RPC Stats", error);
                    return rpcRequest.createResponse()
                            .setStatusCode(RPCStatusCode.ERROR)
                            .setStatusMessage(error.getMessage());
                }

            case "reportHealth":
                try {
                    HealthReport healthReport = amqpServer.reportInternalHealth()
                            .addExtra(amqpServer.getAllMetaData())
                            .addAllAlarms(amqpServer.getAlarms())
                            .setAmqpStats(amqpServer.getAmqpStats());
                    return rpcRequest.createResponse()
                            .setStatusCode(RPCStatusCode.OK)
                            .setResponseBytes(HealthReport.toByteArray(healthReport));
                } catch (IOException error) {
                    logger.error("Failed to report health", error);
                    return rpcRequest.createResponse()
                            .setStatusCode(RPCStatusCode.ERROR)
                            .setStatusMessage(error.getMessage());
                }

            default:
                return routeRPC(rpcRequest);
        }
    }
}
