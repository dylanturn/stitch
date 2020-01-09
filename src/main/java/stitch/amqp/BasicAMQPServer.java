package stitch.amqp;

import com.rabbitmq.client.DeliverCallback;
import org.apache.log4j.Logger;

public class BasicAMQPServer extends BaseAMQPObject implements Runnable {

    static final Logger logger = Logger.getLogger(BasicAMQPServer.class);

    DeliverCallback deliverCallback;

    public BasicAMQPServer(String prefix, String id) {
        super(prefix, id);
        logger.info("Starting up AMQP server...");
        logger.info(String.format("Prefix: %s", prefix));
        logger.info(String.format("Id:     %s", id));
    }

    public void setHandler(DeliverCallback deliverCallback){
        logger.trace("Attaching AMQP delivery callback");
        this.deliverCallback = deliverCallback;
    }

    @Override
    public void run() {
        try {
            logger.trace(String.format("Declaring AMQP queue:             %s", getRouteKey()));
            getChannel().queueDeclare(getRouteKey(), false, false, false, null);
            logger.trace(String.format("Binding to AMQP queue:            %s", getRouteKey()));
            getChannel().queueBind(getRouteKey(), getExchange(), getRouteKey());
            logger.trace(String.format("Listening for AMQP messages from: %s",getHost()));
            getChannel().basicConsume(getRouteKey(), false, deliverCallback, (consumerTag -> {}));
            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized (getMonitor()) {
                    try {
                        getMonitor().wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception error) {
            logger.error("Failed to start the AMQP listener!", error);
        }
    }
}
