package stitch.util;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;

public abstract class EndpointStatus implements Serializable {

    private static final long serialVersionUID = 5470L;

    private String id;
    private long startTime;
    private long reportTime;

    private boolean isHealthy;
    private ArrayList<HealthAlarm> alarms = new ArrayList<>();

    public EndpointStatus(String id, long startTime){
        this.reportTime = Instant.now().toEpochMilli();
        this.id = id;
        this.startTime = startTime;
    }

    public long getReportTime(){
        return reportTime;
    }
    public boolean isHealthy(){
        return isHealthy;
    }
    public String getEndpointId() {
        return id;
    }
    public long getStartTime() { return startTime; }
    public long getEndpointUptime() { return Instant.now().toEpochMilli() - startTime;  }
    public HealthAlarm[] getAlarms(){return alarms.toArray( new HealthAlarm[0]); }


    public EndpointStatus setIsHealthy(boolean isHealthy){
        this.isHealthy = isHealthy;
        return this;
    }

    public EndpointStatus addAlarm(HealthAlarm healthAlarm) {
        alarms.add(healthAlarm);
        return this;
    }

    public EndpointStatus addAllAlarms(ArrayList<HealthAlarm> healthAlarms){
        this.alarms.addAll(healthAlarms);
        return this;
    }

    public static EndpointStatus fromByteArray(byte[] healthStatusBytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(healthStatusBytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        EndpointStatus healthReport = (EndpointStatus) objectInputStream.readObject();
        objectInputStream.close();
        return healthReport;
    }

    public static byte[] toByteArray(EndpointStatus healthReport) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(healthReport);
        objectOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }

}
