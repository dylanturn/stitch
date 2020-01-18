package stitch.amqp.rpc;

import stitch.util.Serializer;

import java.io.IOException;
import java.util.HashMap;

public class RPCRequest extends RPCObject {

    private HashMap<String, Object> arguments = new HashMap<>();

    public RPCRequest(String source, String destination, String method) {
        setSource(source);
        setDestination(destination);
        setMethod(method);
    }

    public RPCRequest(String source, String destination, String method, HashMap<String, Object> arguments){
        this(source, destination, method);
        this.putAllArg(arguments);
    }

    public RPCResponse createResponse(){
        // We flip the source and destination since we're sending it back.
        return new RPCResponse(this.getDestination(), this.getSource(), this.getMethod());
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

    /* -- SERIALIZATION -- */
    public static RPCRequest fromByteArray(byte[] rpcRequestBytes) throws IOException {
        try {
            return (RPCRequest) Serializer.bytesToObject(rpcRequestBytes);
        } catch(ClassNotFoundException error) {
            return null;
        }
    }

    public static byte[] toByteArray(RPCRequest rpcRequest) throws IOException {
        return Serializer.objectToBytes(rpcRequest);
    }

}
