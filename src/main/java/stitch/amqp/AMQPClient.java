package stitch.amqp;

import com.rabbitmq.client.AMQP;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.log4j.Logger;
import stitch.amqp.rpc.RPCPrefix;
import stitch.util.Resource;

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
    private CircularFifoQueue<HealthReport> healthReportQueue;

    public AMQPClient(RPCPrefix prefix, String id) {
        super(prefix, id);
        this.healthReportQueue = new CircularFifoQueue<>(healthReportQueueLength);
        startTimer();
    }

    public byte[] call(String queue, String methodName, Resource resource) throws IOException, InterruptedException {
        return call(queue, methodName, Resource.toByteArray(resource));
    }

    public byte[] call(String queue, String methodName, String resourceString) throws IOException, InterruptedException {
        return call(queue, methodName, resourceString.getBytes());
    }

    public byte[] call(String queue, String methodName, byte[] methodArgs) throws IOException, InterruptedException {
     return this.call(queue, methodName, methodArgs, null);
    }

    public byte[] call(String queue, String methodName, byte[] methodArgs, Map<String, Object> extraHeaders) throws IOException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();

        // Setup the queue that the server will use to reply to the client.
        String replyQueueName = getChannel().queueDeclare().getQueue();

        Map<String, Object> methodHeaders = new HashMap<>();
        methodHeaders.put("caller_prefix", this.getPrefix().toString());
        methodHeaders.put("caller_id", this.getId());
        if(extraHeaders != null) {
            methodHeaders.putAll(extraHeaders);
        }

        // Create the properties that will be sent with the RPC call.
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .type("RPC_" + methodName)
                .headers(methodHeaders)
                .build();

        logger.debug("Queue:       " + queue);
        logger.debug("CorrID:      " + corrId);
        logger.debug("Reply Queue: " + replyQueueName);
        logger.debug("Method:      RPC_" + methodName);

        logger.debug("Publishing the creation RPC...");
        // Publish the RPC call to the queue.
        getChannel().queueDeclare(queue, false, false, false, null);
        getChannel().basicPublish(getExchange(), queue, props, methodArgs);
        final BlockingQueue<Object> response = new ArrayBlockingQueue<>(1);

        String ctag = getChannel().basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(delivery.getBody());
            }
        }, consumerTag -> {
        });

        logger.debug("Waiting for response...");
        byte[] result = (byte[])response.take();
        getChannel().basicCancel(ctag);
        return result;
    }

    private void startTimer(){
        healthReportTimer = new Timer();
        TimerTask task = new CheckHealth();
        healthReportTimer.schedule(task, initialDelay, timerPeriod);
    }

    private class CheckHealth extends TimerTask
    {

        public void run()
        {
            try {
                HealthReport healthReport = reportHealth();
                logger.info("Requesting health report.");
                healthReportQueue.add(healthReport);
                logger.info("Received health report.");
                logger.info(String.format("Node Health: %s", Boolean.toString(healthReport.getIsNodeHealthy())));
            } catch (Exception error) {
                logger.error("Failed to get heartbeat", error);
            }
        }
    }

    public abstract HealthReport reportHealth() throws Exception;

}
