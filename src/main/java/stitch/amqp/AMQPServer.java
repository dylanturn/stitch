package stitch.amqp;

import com.rabbitmq.client.DeliverCallback;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.log4j.Logger;
import stitch.amqp.rpc.RPCPrefix;
import stitch.util.HealthReport;

import java.io.IOException;

public abstract class AMQPServer extends AMQPObject {

    static final Logger logger = Logger.getLogger(AMQPServer.class);

    private DeliverCallback deliverCallback;

    public AMQPServer(RPCPrefix prefix, String id) {
        super(prefix, id);
        logger.info("Starting up AMQP server...");
        logger.info(String.format("Prefix: %s", prefix));
        logger.info(String.format("Id:     %s", id));

        try {
            logger.info(String.format("Declaring AMQP queue:             %s", getRouteKey()));
            getChannel().queueDeclare(getRouteKey(), false, false, false, null);
            logger.info(String.format("Binding to AMQP queue:            %s", getRouteKey()));
            getChannel().queueBind(getRouteKey(), getExchange(), getRouteKey());
        } catch (Exception error) {
            logger.error("Failed to start the AMQP listener!", error);
        }

    }

    public void setHandler(DeliverCallback deliverCallback){
        logger.info("Attaching AMQP delivery callback");
        this.deliverCallback = deliverCallback;
    }

    public void consumeAMQP() {
        try {
            logger.info(String.format("Listening for AMQP messages from: %s", getHost()));
            getChannel().basicConsume(getRouteKey(), false, deliverCallback, (consumerTag -> {
            }));
        } catch (IOException error) {
            logger.error(String.format("Failed to consume channel: %s", getRouteKey()));
        }
        Runnable runnable = () -> {
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
        };
        Thread t = new Thread(runnable);
        t.start();
    }

    public abstract HealthReport reportHealth();

}
