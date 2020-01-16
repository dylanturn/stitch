package stitch.util;

import org.apache.log4j.Logger;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class HealthReport implements Serializable {

    private static final long serialVersionUID = 5470L;

    private static final Logger logger = Logger.getLogger(HealthReport.class);

    private long reportTime;
    private boolean isNodeHealthy;
    private String nodeId;
    private long nodeUptime;

    private ArrayList<HealthAlarm> nodeHealthAlarms = new ArrayList<>();

    public HealthReport(boolean nodeHealthy, String nodeId, long nodeUptime, List<HealthAlarm> healthAlarms){
        this.reportTime = Instant.now().toEpochMilli();
        this.isNodeHealthy = nodeHealthy;
        this.nodeId = nodeId;
        this.nodeUptime = nodeUptime;
        if(healthAlarms != null)
            this.nodeHealthAlarms.addAll(healthAlarms);
    }

    public HealthReport(boolean nodeHealthy, String nodeId, long nodeUptime){
        this(nodeHealthy, nodeId, nodeUptime, null);
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

    public List<HealthAlarm> getAlarms(){
        return nodeHealthAlarms;
    }

    public HealthReport addAlarm(HealthAlarm healthAlarm) {
        nodeHealthAlarms.add(healthAlarm);
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
