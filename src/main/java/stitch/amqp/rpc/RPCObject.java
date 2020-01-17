package stitch.amqp.rpc;

import stitch.util.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public abstract class RPCObject implements Serializable {

    private static final long serialVersionUID = 6891L;

    private String uuid;
    private String source;
    private String destination;
    private String method;
    private long requestStart;
    private long requestEnd;
    private RPCStatusCode statusCode;

    public RPCObject(String source, String destination, String method){
        this.requestStart = Instant.now().toEpochMilli();
        this.uuid = UUID.randomUUID().toString().replace("-", "");
        this.source = source;
        this.destination = destination;
        this.method = method;
    }

    public void completeRequest(RPCStatusCode statusCode) {
        this.requestEnd = Instant.now().toEpochMilli();
        this.statusCode = statusCode;
    }

    public String getUUID() { return uuid; }
    public String getSource() { return source; }
    public String getDestination() { return destination; }
    public String getMethod() { return method; }
    public long getRequestStart() { return requestStart; }
    public long getRequestEnd() { return requestEnd; }
    public RPCStatusCode getStatusCode() { return statusCode; }

    /* -- SERIALIZATION -- */
    public static RPCRequest fromByteArray(byte[] rpcRequestBytes) throws IOException, ClassNotFoundException {
        return (RPCRequest) Serializer.bytesToObject(rpcRequestBytes);
    }

    public static byte[] toByteArray(RPCRequest rpcRequest) throws IOException {
        return Serializer.objectToBytes(rpcRequest);
    }

    public enum ASDFPrefix {
        DATASTORE ("datastore"),
        AGGREGATOR ("aggregator");

        private final String name;
        private ASDFPrefix(String s) { name = s; }
        public boolean equalsName(String otherName) { return name.equals(otherName); }
        public String toString() { return this.name; }
    }

}

