package stitch.amqp.rpc;

import java.io.Serializable;
import java.time.Instant;

public class RPCRequest implements Serializable {

    private static final long serialVersionUID = 6891L;

    private String source;
    private String destination;
    private String method;
    private long requestStart;
    private long requestEnd;
    private RPCStatusCode statusCode;

    public RPCRequest(String source, String destination, String method){
        this.requestStart = Instant.now().toEpochMilli();
        this.source = source;
        this.destination = destination;
        this.method = method;
    }

    public RPCRequest requestComplete(RPCStatusCode statusCode) {
        this.requestEnd = Instant.now().toEpochMilli();
        this.statusCode = statusCode;
        return this;
    }

    public String getSource() { return source; }
    public String getDestination() { return destination; }
    public String getMethod() { return method; }
    public long getRequestStart() { return requestStart; }
    public long getRequestEnd() { return requestEnd; }
    public RPCStatusCode getStatusCode() { return statusCode; }
}
