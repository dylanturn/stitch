package stitch.amqp;

import com.rabbitmq.client.DeliverCallback;
import org.apache.log4j.Logger;
import stitch.amqp.rpc.RPCPrefix;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;

public abstract class AMQPServer extends AMQPObject {

    static final Logger logger = Logger.getLogger(AMQPServer.class);

    private HashMap<String, Object> metaData = new HashMap<>();
    private ArrayList<HealthAlarm> alarms = new ArrayList<>();
    private DeliverCallback deliverCallback;

    public AMQPServer(RPCPrefix prefix, String id) {
        super(prefix, id);
        metaData.put("prefix", prefix);
        metaData.put("uuid", id);
        metaData.put("start_time", Instant.now().toEpochMilli());
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

    public long getNodeUptime() {
        return Instant.now().toEpochMilli() - getMetaLong("start_time");
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

    public HealthReport reportInternalHealth(){
        logger.trace("Generating requested health report.");
        // Create the new health report.
        HealthReport healthReport = new HealthReport(true, getId(), getNodeUptime());
        // Give the implementing class a chance to do stuff with the report.
        reportHealth(healthReport);
        // Return the report.
        return healthReport;
    }

    /* EXTRA DATA */
    public Object getMetaData(String key) {
        return this.metaData.get(key);
    }

    public String getMetaString(String key) {
        return (String)this.metaData.get(key);
    }

    public int getMetaInt(String key) {
        return (int)this.metaData.get(key);
    }

    public long getMetaLong(String key) {
        return (long)this.metaData.get(key);
    }

    public boolean getMetaBoolean(String key) {
        return (boolean)this.metaData.get(key);
    }

    public HashMap<String, Object> getAllMetaData() {
        return this.metaData;
    }

    public void addMetaData(String key, Object value){
        this.metaData.put(key, value);
    }

    public void addAllMetaData(HashMap<String, Object> allExtra){
        this.metaData.putAll(allExtra);
    }

    /* HEALTH ALARMS */
    public ArrayList<HealthAlarm> getAlarms() {
        return this.alarms;
    }

    public void addAlarm(HealthAlarm alarm){
        this.alarms.add(alarm);
    }

    public void addAllAlarms(ArrayList<HealthAlarm> alarms){
        this.alarms.addAll(alarms);
    }

    public abstract void reportHealth(HealthReport healthReport);

}
