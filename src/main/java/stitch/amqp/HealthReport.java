package stitch.amqp;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HealthReport implements Serializable {

    private static final long serialVersionUID = 5470L;

    private long reportTime;
    private boolean isNodeHealthy;
    private String nodeId;
    private long nodeUptime;
    private AMQPStats amqpStats;
    private ArrayList<HealthAlarm> alarms = new ArrayList<>();
    private HashMap<String, Object> extraData = new HashMap<>();

    public HealthReport(boolean nodeHealthy, String nodeId, long nodeUptime, List<HealthAlarm> healthAlarms, AMQPStats amqpStats){
        this.reportTime = Instant.now().toEpochMilli();
        this.isNodeHealthy = nodeHealthy;
        this.nodeId = nodeId;
        this.nodeUptime = nodeUptime;
        if(healthAlarms != null)
            this.alarms.addAll(healthAlarms);
        if(amqpStats != null)
            this.amqpStats = amqpStats;
    }

    public HealthReport(boolean nodeHealthy, String nodeId, long nodeUptime){
        this(nodeHealthy, nodeId, nodeUptime, null, null);
    }

    public HealthReport(boolean nodeHealthy, String nodeId, long nodeUptime, List<HealthAlarm> healthAlarms){
        this(nodeHealthy, nodeId, nodeUptime, healthAlarms, null);
    }

    public HealthReport(boolean nodeHealthy, String nodeId, long nodeUptime, AMQPStats amqpStats){
        this(nodeHealthy, nodeId, nodeUptime, null, amqpStats);
    }

    public long getReportTime(){
        return reportTime;
    }

    public boolean getIsNodeHealthy(){
        return isNodeHealthy;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getNodeUptime() {
        return nodeUptime;
    }

    /* EXTRA DATA */
    public HashMap<String, Object> getExtra() { return extraData; }

    public HealthReport addExtra(String key, Object value) {
        this.extraData.put(key, value);
        return this;
    }

    public HealthReport addExtra(HashMap<String, Object> extraData) {
        this.extraData.putAll(extraData);
        return this;
    }

    /* HEALTH ALARMS */
    public List<HealthAlarm> getAlarms(){
        return alarms;
    }

    public HealthReport addAlarm(HealthAlarm healthAlarm) {
        alarms.add(healthAlarm);
        return this;
    }

    public HealthReport addAllAlarms(ArrayList<HealthAlarm> nodeHealthAlarms){
        this.alarms.addAll(nodeHealthAlarms);
        return this;
    }

    /* RPC STATS */
    public AMQPStats getAmqpStats() { return amqpStats; }

    public HealthReport setAmqpStats(AMQPStats amqpStats) {
        this.amqpStats = amqpStats;
        return this;
    }

    public static HealthReport fromByteArray(byte[] healthStatusBytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(healthStatusBytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        HealthReport healthReport = (HealthReport) objectInputStream.readObject();
        objectInputStream.close();
        return healthReport;
    }

    public static byte[] toByteArray(HealthReport healthReport) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(healthReport);
        objectOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }

}
