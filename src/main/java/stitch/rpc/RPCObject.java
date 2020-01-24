package stitch.rpc;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public abstract class RPCObject implements Serializable {

    private static final long serialVersionUID = 6891L;

    private String uuid;
    private String source;
    private String destination;
    private Class<?> methodClass;
    private String method;
    private long requestStart;
    private long requestEnd;

    public RPCObject(){
        this.requestStart = Instant.now().toEpochMilli();
        this.uuid = UUID.randomUUID().toString().replace("-", "");
    }

    public String getUUID() { return uuid; }
    public String getSource() { return source; }
    public String getDestination() { return destination; }
    public Class<?> getMethodClass() { return methodClass; }
    public String getMethod() { return method; }
    public long getRequestStart() { return requestStart; }
    public long getRequestEnd() { return requestEnd; }

    public RPCObject setSource(String rpcSource){
        source = rpcSource;
        return this;
    }
    public RPCObject setDestination(String rpcDestination){
        destination = rpcDestination;
        return this;
    }
    public RPCObject setMethodClass(Class<?> methodClass){
        this.methodClass = methodClass;
        return this;
    }
    public RPCObject setMethod(String rpcMethod){
        method = rpcMethod;
        return this;
    }
}

