package stitch.util;

import org.apache.log4j.Logger;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class HealthReport {

    private static final long serialVersionUID = 1234L;
    private static final Logger logger = Logger.getLogger(HealthReport.class);

    private boolean isNodeHealthy;
    private long lastHealthReportTime;
    private ArrayList<Alarm> NodeAlarms = new ArrayList<>();

    public HealthReport(boolean nodeHealthy, List<Alarm> alarms){
        this.isNodeHealthy = nodeHealthy;
        this.lastHealthReportTime = Instant.now().toEpochMilli();
        this.addAlarms(alarms);
    }

    public HealthReport(boolean nodeHealthy){
        this.isNodeHealthy = nodeHealthy;
        this.lastHealthReportTime = Instant.now().toEpochMilli();
    }

    public void addAlarms(List<Alarm> alarms){
        this.NodeAlarms.addAll(alarms);
    }

    public List<Alarm> getAlarms(){
        return this.NodeAlarms;
    }

    public long getLastHealthReportTime(){
        return lastHealthReportTime;
    }

    public boolean getIsNodeHealthy(){
        return isNodeHealthy;
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

    public class Alarm {

    }


}
