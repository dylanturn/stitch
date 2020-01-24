package stitch.rpc.transport.metrics;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.javatuples.Triplet;
import stitch.rpc.RPCObject;
import stitch.rpc.RPCRequest;
import stitch.rpc.RPCResponse;
import stitch.rpc.RPCStatusCode;
import stitch.util.Serializer;

import java.io.*;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;

public class RpcEndpointReporter implements Serializable {

    private static final long serialVersionUID = 1986L;

    // RPC Calls
    private long totalCalls = 0;
    private long successCount = 0;
    private long failuresCount = 0;

    // Triplet stores rates for 1 minute, 5 minutes, and 15 minutes.
    private Triplet<Long,Long,Long> callRate =  new Triplet<>(0L,0L,0L);
    private Triplet<Long,Long,Long> responseTime =  new Triplet<>(0L,0L,0L);
    private Triplet<Long,Long,Long> successRate =  new Triplet<>(0L,0L,0L);
    private Triplet<Long,Long,Long> failureRate =  new Triplet<>(0L,0L,0L);

    // Circular buffer of the last n number of RPC calls.
    private CircularFifoQueue<RPCObject> callRecordQueue;

    public RpcEndpointReporter(int queueLength) {
        callRecordQueue = new CircularFifoQueue<>(queueLength);
        //startTimer();
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

    public static RpcEndpointReporter fromByteArray(byte[] rpcStatsbytes) throws IOException, ClassNotFoundException {
        return (RpcEndpointReporter)Serializer.bytesToObject(rpcStatsbytes);
    }

    public static byte[] toByteArray(RpcEndpointReporter rpcStats) throws IOException {
        return Serializer.objectToBytes(rpcStats);
    }

   /* private void startTimer(){
        healthReportTimer = new Timer();
        TimerTask task = new CheckHealth();
        healthReportTimer.schedule(task, initialDelay, timerPeriod);
    }

    public RpcEndpointReport getLastHealthReport(){
        return lastHealthReport;
    }

    public Iterator<RpcEndpointReport> getAllHealthReports(){
        return healthReportQueue.iterator();
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

    private RpcEndpointReport reportHealth() throws Exception {
        return (RpcEndpointReport)invokeRPC(new RPCRequest("", getRouteKey(), "reportHealth"))
                .getResponseObject();
    }*/

}
