package stitch.rpc;

import stitch.resource.Resource;
import stitch.util.Serializer;

import java.io.IOException;
import java.util.HashMap;

public class RPCRequest extends RPCObject {

    private HashMap<Class<?>, Object> arguments = new HashMap<>();

    public RPCRequest(String source, String destination, String method) {
        setSource(source);
        setDestination(destination);
        setMethod(method);
    }

    public RPCResponse createResponse(){
        // We flip the source and destination since we're sending it back.
        return new RPCResponse(this.getDestination(), this.getSource(), this.getMethod());
    }

    /* -- RPC ARGS -- */
    public Object getArg(String key){ return arguments.get(key); }
    public String getStringArg(String key){ return (String)getArg(key); }
    public int getIntArg(String key){ return (int)getArg(key); }
    public long getLongArg(String key){ return (long)getArg(key); }
    public boolean getBoolArg(String key){ return (boolean)getArg(key); }
    public Resource getResourceArg(String key){ return (Resource) getArg(key); }

    public Object[] getArgValues(){
        return arguments.values().toArray();
    }

    public Class<?>[] getArgClasses(){
        return arguments.keySet().toArray(new Class[arguments.size()]);
    }

    public RPCRequest putArg(Class<?> key, Object value) {
        arguments.put(key, value);
        return this;
    }

    public RPCRequest putStringArg(Object value) {
        arguments.put(String.class, value);
        return this;
    }

    public RPCRequest putIntArg(Object value) {
        arguments.put(int.class, value);
        return this;
    }

    public RPCRequest putLongArg(Object value) {
        arguments.put(long.class, value);
        return this;
    }

    public RPCRequest putBoolArg(Object value) {
        arguments.put(boolean.class, value);
        return this;
    }

    public RPCRequest putResourceArg(Object value) {
        arguments.put(Resource.class, value);
        return this;
    }

    public RPCRequest putAllArg(HashMap<Class<?>, Object> extraArguments) {
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
