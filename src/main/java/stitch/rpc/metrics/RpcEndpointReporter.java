package stitch.rpc.metrics;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.log4j.Logger;
import org.javatuples.Triplet;
import stitch.rpc.RPCObject;
import stitch.rpc.RPCResponse;
import stitch.rpc.RPCStatusCode;
import stitch.rpc.transport.amqp.AMQPClient;
import stitch.util.Serializer;
import stitch.util.configuration.item.ConfigItem;

import java.io.*;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class RpcEndpointReporter implements Serializable {

    private static final Logger logger = Logger.getLogger(AMQPClient.class);
    private static final long serialVersionUID = 1986L;

    private long startTime;
    private String endpointId;
    private boolean isHealthy = true;

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
    private List<RpcEndpointAlarm> rpcEndpointAlarms;

    // Circular buffer of the last n number of RPC calls.
    private CircularFifoQueue<RPCObject> callRecordQueue;

    public RpcEndpointReporter(ConfigItem endpointConfig) {
        startTime = Instant.now().toEpochMilli();
        this.endpointId = endpointConfig.getConfigId();
        callRecordQueue = new CircularFifoQueue<>(100);
        //callRecordQueue = new CircularFifoQueue<>(endpointConfig.getConfigInt("reporter_queue_size"));
    }

    public long getTotalCalls() { return totalCalls; }
    public long getSuccessCount() { return successCount; }
    public long getFailuresCount() { return failuresCount; }

    public Triplet<Long, Long, Long> getCallRate() { return callRate; }
    public Triplet<Long, Long, Long> getResponseTime() { return responseTime; }
    public Triplet<Long, Long, Long> getSuccessRate() { return successRate; }
    public Triplet<Long, Long, Long> getFailureRate() { return failureRate; }

    public Iterator<RPCObject> getCallRecords() { return callRecordQueue.iterator(); }
    public RPCObject getLatestCallRecord(){ return callRecordQueue.peek(); }

    public void queueCallRecord(RPCResponse callRecord) {

        // Increment the total number of calls received.
        this.totalCalls++;

        // Increment the success and failure counters. (I added braces because this isn't Python)
        if(callRecord.getStatusCode() == RPCStatusCode.OK) {
            this.successCount++;
        } else {
            this.failuresCount++;
        }

        // Add the call record to the circular buffer.
        callRecordQueue.add(callRecord);
    }

    public RpcEndpointReport generateReport() {
        long endpointUptime = Instant.now().toEpochMilli() - startTime;
        return new RpcEndpointReport(endpointId, isHealthy, endpointUptime);
    }

    public static RpcEndpointReporter fromByteArray(byte[] rpcStatsbytes) throws IOException, ClassNotFoundException {
        return (RpcEndpointReporter)Serializer.bytesToObject(rpcStatsbytes);
    }

    public static byte[] toByteArray(RpcEndpointReporter rpcStats) throws IOException {
        return Serializer.objectToBytes(rpcStats);
    }

    /*

    Timer endpointReportTimer;
    int timerInitialDelay = 5000;
    int timerReportPeriod = 5000;

    private void startTimer(){
        endpointReportTimer = new Timer();
        TimerTask task = new CheckHealth();
        endpointReportTimer.schedule(task, timerInitialDelay, timerReportPeriod);
    }

    private class CheckHealth extends TimerTask
    {

        public void run()
        {
            try {
                RpcEndpointReport healthReport = reportHealth();
                logger.trace("Requesting health report.");
                lastHealthReport = healthReport;
                healthReportQueue.add(healthReport);
                logger.trace("Received health report.");
                logger.trace(String.format("Node Health: %s", Boolean.toString(healthReport.getIsNodeHealthy())));
            } catch (Exception error) {
                logger.error("Failed to get heartbeat", error);
            }
        }
    }

    */
}
