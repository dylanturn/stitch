package stitch.util;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.javatuples.Triplet;
import stitch.rpc.Rpc;
import stitch.rpc.RpcResponse;
import stitch.rpc.RpcStatusCode;
import stitch.util.configuration.item.ConfigItem;

import java.io.*;
import java.time.Instant;
import java.util.*;

public class EndpointMetrics implements Serializable {

    private static final long serialVersionUID = 1986L;

    private long startTime;
    private String endpointId;

    // RPC Calls
    private long totalCalls = 0;
    private long successCount = 0;
    private long failuresCount = 0;

    // Triplet stores rates for 1 minute, 5 minutes, and 15 minutes.
    private Triplet<Long,Long,Long> callRate =  new Triplet<>(0L,0L,0L);
    private Triplet<Long,Long,Long> responseTime =  new Triplet<>(0L,0L,0L);
    private Triplet<Long,Long,Long> successRate =  new Triplet<>(0L,0L,0L);
    private Triplet<Long,Long,Long> failureRate =  new Triplet<>(0L,0L,0L);

    // List of alarms applicable to this endpoint.
    private List<HealthAlarm> healthAlarms;
    private HashMap<String, Object> extraData = new HashMap<>();

    // Circular buffer of the last n number of RPC calls.
    private CircularFifoQueue<Rpc> callRecordQueue;

    public EndpointMetrics(ConfigItem endpointConfig) {
        startTime = Instant.now().toEpochMilli();
        this.endpointId = endpointConfig.getConfigId();
        callRecordQueue = new CircularFifoQueue<>(100);
        //callRecordQueue = new CircularFifoQueue<>(config.getConfigInt("reporter_queue_size"));
    }

    public long getStartTime() {
        return startTime;
    }
    public String getEndpointId(){
        return endpointId;
    }

    public long getTotalCalls() { return totalCalls; }
    public long getSuccessCount() { return successCount; }
    public long getFailuresCount() { return failuresCount; }

    public Triplet<Long, Long, Long> getCallRate() { return callRate; }
    public Triplet<Long, Long, Long> getResponseTime() { return responseTime; }
    public Triplet<Long, Long, Long> getSuccessRate() { return successRate; }
    public Triplet<Long, Long, Long> getFailureRate() { return failureRate; }

    public Iterator<Rpc> getCallRecords() { return callRecordQueue.iterator(); }
    public Rpc getLatestCallRecord(){ return callRecordQueue.peek(); }

    public void queueCallRecord(RpcResponse callRecord) {

        // Increment the total number of calls received.
        this.totalCalls++;

        // Increment the success and failure counters. (I added braces because this isn't Python)
        if(callRecord.getStatusCode() == RpcStatusCode.OK) {
            this.successCount++;
        } else {
            this.failuresCount++;
        }

        // Add the call record to the circular buffer.
        callRecordQueue.add(callRecord);
    }

    /* EXTRA DATA */
    public HashMap<String, Object> getExtra() {
        return extraData;
    }

    public void addExtra(String key, Object value) {
        extraData.put(key, value);
    }

    public void addExtra(HashMap<String, Object> extraData) {
        extraData.putAll(extraData);
    }

    /* HEALTH ALARMS */
    public HealthAlarm[] getAlarms(){
        return healthAlarms.toArray( new HealthAlarm[0] );
    }

    public void addAlarm(HealthAlarm healthAlarm) {
        healthAlarms.add(healthAlarm);
    }

    public void addAlarm(ArrayList<HealthAlarm> healthAlarms){
       this.healthAlarms.addAll(healthAlarms);
    }

    public EndpointMetrics getRpcEndpointReporter(){
        return this;
    }

    public static EndpointMetrics fromByteArray(byte[] rpcStatsbytes) throws IOException, ClassNotFoundException {
        return (EndpointMetrics)Serializer.bytesToObject(rpcStatsbytes);
    }

    public static byte[] toByteArray(EndpointMetrics rpcStats) throws IOException {
        return Serializer.objectToBytes(rpcStats);
    }
}
