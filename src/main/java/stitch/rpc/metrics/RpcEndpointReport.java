package stitch.rpc.metrics;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RpcEndpointReport implements Serializable {

    private static final long serialVersionUID = 5470L;

    private long reportTime;
    private boolean isNodeHealthy;
    private String nodeId;
    private long nodeUptime;
    private RpcEndpointReporter rpcStats;
    private ArrayList<RpcEndpointAlarm> alarms = new ArrayList<>();
    private HashMap<String, Object> extraData = new HashMap<>();

    public RpcEndpointReport(String nodeId, boolean nodeHealthy, long nodeUptime){
        this.reportTime = Instant.now().toEpochMilli();
        this.isNodeHealthy = nodeHealthy;
        this.nodeId = nodeId;
        this.nodeUptime = nodeUptime;
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

    public RpcEndpointReport addExtra(String key, Object value) {
        this.extraData.put(key, value);
        return this;
    }

    public RpcEndpointReport addExtra(HashMap<String, Object> extraData) {
        this.extraData.putAll(extraData);
        return this;
    }

    /* HEALTH ALARMS */
    public List<RpcEndpointAlarm> getAlarms(){
        return alarms;
    }

    public RpcEndpointReport addAlarm(RpcEndpointAlarm rpcEndpointAlarm) {
        alarms.add(rpcEndpointAlarm);
        return this;
    }

    public RpcEndpointReport addAllAlarms(ArrayList<RpcEndpointAlarm> nodeRpcEndpointAlarms){
        this.alarms.addAll(nodeRpcEndpointAlarms);
        return this;
    }

    /* RPC STATS */
    public RpcEndpointReporter getRpcStats() { return rpcStats; }

    public RpcEndpointReport setRpcStats(RpcEndpointReporter rpcStats) {
        this.rpcStats = rpcStats;
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
