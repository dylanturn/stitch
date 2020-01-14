package stitch.amqp.rpc;

import java.time.Instant;

public class RPCRecord {

    private String caller;
    private String method;
    private long callStart;
    private long callEnd;
    private RPCStatusCode result;
    private RPCStats rpcStats;

    public RPCRecord(String caller, String method, RPCStats rpcStats){
        this.callStart = Instant.now().toEpochMilli();
        this.caller = caller;
        this.method = method;
        this.rpcStats = rpcStats;
    }

    public void endCall(RPCStatusCode result) {
        this.callEnd = Instant.now().toEpochMilli();
        this.result = result;
        this.rpcStats.queueCallRecord(this);
    }

    public String getCaller() { return caller; }
    public String getMethod() { return method; }
    public long getCallStart() { return callStart; }
    public long getCallEnd() { return callEnd; }
    public RPCStatusCode getResult() { return result; }
}
