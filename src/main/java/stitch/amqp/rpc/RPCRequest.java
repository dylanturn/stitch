package stitch.amqp.rpc;

import java.util.HashMap;

public class RPCRequest extends RPCObject {


    private HashMap<String, Object> arguments = new HashMap<>();

    public RPCRequest(String source, String destination, String method) {
        super(source, destination, method);
    }

    public RPCRequest(String source, String destination, String method, HashMap<String, Object> arguments){
        this(source, destination, method);
        this.putAllArg(arguments);
    }

    public RPCResponse createResponse(RPCStatusCode rpcStatusCode){
        completeRequest(rpcStatusCode);
        return new RPCResponse(this);
    }

    public RPCResponse createResponse(RPCStatusCode rpcStatusCode, byte[] responseBytes){
        return createResponse(rpcStatusCode).setResponseBytes(responseBytes);
    }

    public RPCResponse createResponse(RPCStatusCode rpcStatusCode, Object responseObject){
        return createResponse(rpcStatusCode).setResponseObject(responseObject);
    }

    /* -- RPC ARGS -- */
    public Object getArg(String key){ return arguments.get(key); }
    public String getStringArg(String key){ return (String)getArg(key); }
    public boolean getBoolArg(String key){ return (boolean)getArg(key); }
    public int getIntArg(String key){ return (int)getArg(key); }
    public long getLongArg(String key){ return (long)getArg(key); }
    public RPCRequest putArg(String key, Object value) {
        arguments.put(key, value);
        return this;
    }
    public RPCRequest putAllArg(HashMap<String, Object> extraArguments) {
        arguments.putAll(extraArguments);
        return this;
    }

}
