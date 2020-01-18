package stitch.amqp;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.javatuples.Triplet;
import stitch.amqp.rpc.RPCRecord;
import stitch.amqp.rpc.RPCStatusCode;
import stitch.util.Serializer;

import java.io.*;
import java.util.Iterator;

public class AMQPStats implements Serializable {

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
    private CircularFifoQueue<RPCRecord> callRecordQueue;

    public AMQPStats(int queueLength) {
        callRecordQueue = new CircularFifoQueue<>(queueLength);
    }

    public long getTotalCalls() { return totalCalls; }
    public long getSuccessCount() { return successCount; }
    public long getFailuresCount() { return failuresCount; }

    public Triplet<Long, Long, Long> getCallRate() { return callRate; }
    public Triplet<Long, Long, Long> getResponseTime() { return responseTime; }
    public Triplet<Long, Long, Long> getSuccessRate() { return successRate; }
    public Triplet<Long, Long, Long> getFailureRate() { return failureRate; }

    public Iterator<RPCRecord> getCallRecords() { return callRecordQueue.iterator(); }
    public RPCRecord getLatestCallRecord(){ return callRecordQueue.peek(); }

    public void queueCallRecord(RPCRecord callRecord) {

        // Increment the total number of calls received.
        this.totalCalls++;

        // Increment the success and failure counters. (I added braces because this isn't Python)
        if(callRecord.getResult() == RPCStatusCode.OK) {
            this.successCount++;
        } else {
            this.failuresCount++;
        }

        // Add the call record to the circular buffer.
        callRecordQueue.add(callRecord);
    }

    public static AMQPStats fromByteArray(byte[] rpcStatsbytes) throws IOException, ClassNotFoundException {
        return (AMQPStats)Serializer.bytesToObject(rpcStatsbytes);
    }

    public static byte[] toByteArray(AMQPStats amqpStats) throws IOException {
        return Serializer.objectToBytes(amqpStats);
    }

}
