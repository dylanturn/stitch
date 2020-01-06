package stitch.amqp;

import com.rabbitmq.client.DeliverCallback;
import org.apache.log4j.Logger;

public class BasicAMQPServer extends BaseAMQPObject implements Runnable {

    static final Logger logger = Logger.getLogger(BasicAMQPServer.class);

    DeliverCallback deliverCallback;

    public BasicAMQPServer(String prefix, String id) {
        super(prefix, id);
    }

    public void setHandler(DeliverCallback deliverCallback){
        this.deliverCallback = deliverCallback;
    }

    @Override
    public void run() {
        try {
            getChannel().queueDeclare(getRouteKey(), false, false, false, null);
            getChannel().queueBind(getRouteKey(), getExchange(), getRouteKey());
            logger.info("Listening for AMQP messages from: " + getHost());
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
