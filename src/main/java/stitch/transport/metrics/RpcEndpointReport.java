package stitch.transport.metrics;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;

public abstract class RpcEndpointReport implements Serializable {

    private static final long serialVersionUID = 5470L;

    private long reportTime;
    private boolean isNodeHealthy;
    private String endpointId;
    private long endpointUptime;
    private ArrayList<RpcEndpointAlarm> alarms = new ArrayList<>();

    public RpcEndpointReport(RpcEndpointReporter rpcEndpointReporter){
        this.reportTime = Instant.now().toEpochMilli();
        this.isNodeHealthy = rpcEndpointReporter.isHealthy();
        this.endpointId = rpcEndpointReporter.getEndpointId();
        this.endpointUptime = Instant.now().toEpochMilli() - rpcEndpointReporter.getStartTime();
    }

    public long getReportTime(){
        return reportTime;
    }

    public boolean isHealthy(){
        return isNodeHealthy;
    }

    public String getEndpointId() {
        return endpointId;
    }

    public long getEndpointUptime() {
        return endpointUptime;
    }

    /* HEALTH ALARMS */
    public RpcEndpointAlarm[] getAlarms(){return alarms.toArray( new RpcEndpointAlarm[0]); }

    public RpcEndpointReport addAlarm(RpcEndpointAlarm rpcEndpointAlarm) {
        alarms.add(rpcEndpointAlarm);
        return this;
    }

    public RpcEndpointReport addAllAlarms(ArrayList<RpcEndpointAlarm> nodeRpcEndpointAlarms){
        this.alarms.addAll(nodeRpcEndpointAlarms);
        return this;
    }

    public static RpcEndpointReport fromByteArray(byte[] healthStatusBytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(healthStatusBytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        RpcEndpointReport healthReport = (RpcEndpointReport) objectInputStream.readObject();
        objectInputStream.close();
        return healthReport;
    }

    public static byte[] toByteArray(RpcEndpointReport healthReport) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(healthReport);
        objectOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }

}
