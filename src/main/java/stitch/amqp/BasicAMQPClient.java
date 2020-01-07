package stitch.amqp;

import com.rabbitmq.client.AMQP;
import org.apache.log4j.Logger;
import stitch.util.Resource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BasicAMQPClient extends BaseAMQPObject {

    static final Logger logger = Logger.getLogger(BasicAMQPClient.class);

    public BasicAMQPClient(String prefix, String id) {
        super(prefix, id);
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
        methodHeaders.put("caller_prefix", this.getPrefix());
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


}
