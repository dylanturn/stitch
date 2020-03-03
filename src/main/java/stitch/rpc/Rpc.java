package stitch.rpc;

import stitch.transport.TransmitMode;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public abstract class Rpc implements Serializable {

    private static final long serialVersionUID = 6891L;

    private String uuid;
    private String source;
    private String destination;
    private Class<?> methodClass;
    private String method;
    private TransmitMode transmitMode;
    private long requestStart;
    private long requestEnd;

    public Rpc(){
        this.requestStart = Instant.now().toEpochMilli();
        this.uuid = UUID.randomUUID().toString().replace("-", "");
    }

    public String getUUID() { return uuid; }
    public String getSource() { return source; }
    public String getDestination() { return destination; }
    public Class<?> getMethodClass() { return methodClass; }
    public String getMethod() { return method; }
    public TransmitMode getTransmitMode() { return transmitMode; }
    public long getRequestStart() { return requestStart; }
    public long getRequestEnd() { return requestEnd; }

    public Rpc setSource(String rpcSource){
        source = rpcSource;
        return this;
    }
    public Rpc setDestination(String rpcDestination){
        destination = rpcDestination;
        return this;
    }
    public Rpc setMethodClass(Class<?> rpcMethodClass){
        methodClass = rpcMethodClass;
        return this;
    }
    public Rpc setMethod(String rpcMethod){
        method = rpcMethod;
        return this;
    }
    public Rpc setTransmitMode(TransmitMode rpcTransmitMode) {
        transmitMode = rpcTransmitMode;
        return this;
    }
}

