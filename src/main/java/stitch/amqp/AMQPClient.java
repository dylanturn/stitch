package stitch.amqp;

import com.rabbitmq.client.AMQP;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.log4j.Logger;
import stitch.amqp.rpc.RPCRequest;
import stitch.amqp.rpc.RPCResponse;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class AMQPClient extends AMQPObject {

    static final Logger logger = Logger.getLogger(AMQPClient.class);

    private Timer healthReportTimer;
    private int initialDelay = 5000;
    private int timerPeriod = 5000;
    private int healthReportQueueLength = 100;

    private HealthReport lastHealthReport;
    private CircularFifoQueue<HealthReport> healthReportQueue;

    public AMQPClient(AMQPPrefix prefix, String id) {
        super(prefix, id);
        this.healthReportQueue = new CircularFifoQueue<>(healthReportQueueLength);
        startTimer();
    }

    public RPCResponse invokeRPC(RPCRequest rpcRequest) throws IOException, InterruptedException {

        // Setup the queue and correlation id that the server will use to reply to the client.
        final String corrId = UUID.randomUUID().toString();
        String replyQueueName = getChannel().queueDeclare().getQueue();

        // Create the properties that will be sent with the RPC call.
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        logger.debug("Queue:       " + rpcRequest.getDestination());
        logger.debug("CorrID:      " + corrId);
        logger.debug("Reply Queue: " + replyQueueName);
        logger.debug("Method:      RPC_" + rpcRequest.getMethod());

        logger.debug("Publishing the creation RPC...");
        // Publish the RPC call to the queue.
        getChannel().queueDeclare(rpcRequest.getDestination(), false, false, false, null);
        getChannel().basicPublish(getExchange(), rpcRequest.getDestination(), props, RPCRequest.toByteArray(rpcRequest));
        final BlockingQueue<Object> response = new ArrayBlockingQueue<>(1);

        String ctag = getChannel().basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                try {
                    response.offer(RPCResponse.fromByteArray(delivery.getBody()));
                } catch( ClassNotFoundException error) {
                    logger.error("Failed to load RPC response from bytes", error);
                }
            }
        }, consumerTag -> {
        });

        logger.debug("Waiting for response...");
        RPCResponse rpcResponse = (RPCResponse)response.take();
        getChannel().basicCancel(ctag);
        return rpcResponse;
    }

    private void startTimer(){
        healthReportTimer = new Timer();
        TimerTask task = new CheckHealth();
        healthReportTimer.schedule(task, initialDelay, timerPeriod);
    }

    public HealthReport getLastHealthReport(){
        return lastHealthReport;
    }

    public Iterator<HealthReport> getAllHealthReports(){
        return healthReportQueue.iterator();
    }

    private class CheckHealth extends TimerTask
    {

        public void run()
        {
            try {
                HealthReport healthReport = reportHealth();
                logger.trace("Requesting health report.");
                lastHealthReport = healthReport;
                healthReportQueue.add(healthReport);
                logger.trace("Received health report.");
                logger.trace(String.format("Node Health: %s", Boolean.toString(healthReport.getIsNodeHealthy())));
            } catch (Exception error) {
                logger.error("Failed to get heartbeat", error);
            }
        }
    }

    private HealthReport reportHealth() throws Exception {
        return (HealthReport)invokeRPC(new RPCRequest("", getRouteKey(), "reportHealth"))
                .getResponseObject();
    }

}
