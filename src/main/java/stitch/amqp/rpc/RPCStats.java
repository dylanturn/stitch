package stitch.amqp.rpc;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.javatuples.Triplet;

import java.io.*;
import java.time.Instant;
import java.util.Iterator;

public class RPCStats {
    // RPC Calls
    private long totalCalls;
    private long queueLength;
    private long successCount;
    private long failuresCount;

    // Triplet stores rates for 1 minute, 5 minutes, and 15 minutes.
    private Triplet<Long,Long,Long> callRate;
    private Triplet<Long,Long,Long> responseTime;
    private Triplet<Long,Long,Long> successRate;
    private Triplet<Long,Long,Long> failureRate;

    // Circular buffer of the last n number of RPC calls.
    private CircularFifoQueue<CallRecord> callRecordQueue;

    public RPCStats(int queueLength) {
        callRecordQueue = new CircularFifoQueue<>(queueLength);
    }

    public long getTotalCalls() { return totalCalls; }
    public long getQueueLength() { return queueLength; }
    public long getSuccessCount() { return successCount; }
    public long getFailuresCount() { return failuresCount; }

    public Triplet<Long, Long, Long> getCallRate() { return callRate; }
    public Triplet<Long, Long, Long> getResponseTime() { return responseTime; }
    public Triplet<Long, Long, Long> getSuccessRate() { return successRate; }
    public Triplet<Long, Long, Long> getFailureRate() { return failureRate; }

    public void addCallRecord(CallRecord callRecord) { callRecordQueue.add(callRecord); }

    public Iterator<CallRecord> getCallRecords() {
        return callRecordQueue.iterator();
    }

    public CallRecord getLatestCallRecord(){
        return callRecordQueue.peek();
    }

    public class CallRecord {

        private String caller;
        private String method;
        private long callStart;
        private long callEnd;
        private RPCObject.RPCResponseCode result;

        public CallRecord(String caller, String method){
            this.callStart = Instant.now().toEpochMilli();
            this.caller = caller;
            this.method = method;
        }

        public void EndCall(RPCObject.RPCResponseCode result) {
            this.callEnd = Instant.now().toEpochMilli();
            this.result = result;
        }
    }

    public static RPCStats fromByteArray(byte[] rpcStatsbytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(rpcStatsbytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        RPCStats rpcStats = (RPCStats) objectInputStream.readObject();
        objectInputStream.close();
        return rpcStats;
    }

    public static byte[] toByteArray(RPCStats rpcStats) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(rpcStats);
        objectOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }

}
