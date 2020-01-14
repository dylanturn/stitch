package stitch.amqp.rpc;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.log4j.Logger;
import stitch.util.HealthReport;

import java.util.Timer;
import java.util.TimerTask;

public abstract class RPCObject {

    static final Logger logger = Logger.getLogger(RPCObject.class);

    private RPCPrefix prefix;
    private String id;
    private int callRecordQueueLength = 100;
    private RPCStats rpcStats;

    public RPCObject(RPCPrefix prefix, String id){
        this.prefix = prefix;
        this.id = id;
        this.rpcStats = new RPCStats(callRecordQueueLength);
    }

    public RPCPrefix getPrefix(){
        return this.prefix;
    }
    public String getPrefixString(){
        return this.prefix.toString();
    }
    public String getId(){
        return this.id;
    }
    public String getRouteKey(){
        return String.format("%s_%s", this.getPrefixString(), this.getId());
    }

    // Is this ok?
    public RPCRecord startRPC(String caller, String method){
        return new RPCRecord(caller, method, rpcStats);
    }

    public RPCStats getRpcStats() {
        return rpcStats;
    }

}
