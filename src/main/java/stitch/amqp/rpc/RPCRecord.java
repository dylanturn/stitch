package stitch.amqp.rpc;

import stitch.amqp.AMQPStats;

import java.io.Serializable;
import java.time.Instant;

public class RPCRecord implements Serializable {

    private static final long serialVersionUID = 1721L;

    private String caller;
    private String method;
    private long callStart;
    private long callEnd;
    private RPCStatusCode result;
    private AMQPStats amqpStats;

    public RPCRecord(String caller, String method, AMQPStats amqpStats){
        this.callStart = Instant.now().toEpochMilli();
        this.caller = caller;
        this.method = method;
        this.amqpStats = amqpStats;
    }

    public void endCall(RPCStatusCode result) {
        this.callEnd = Instant.now().toEpochMilli();
        this.result = result;
        this.amqpStats.queueCallRecord(this);
    }

    public String getCaller() { return caller; }
    public String getMethod() { return method; }
    public long getCallStart() { return callStart; }
    public long getCallEnd() { return callEnd; }
    public RPCStatusCode getResult() { return result; }
}
