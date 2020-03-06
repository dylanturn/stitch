package stitch.rpc;

import stitch.datastore.resource.Resource;
import stitch.transport.TransmitMode;
import stitch.util.Serializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class RpcRequest extends Rpc {

    private LinkedHashMap<Class<?>, Object> arguments = new LinkedHashMap<>();

    public RpcRequest(String source, String destination, String method) {
        setSource(source);
        setDestination(destination);
        setMethod(method);
    }

    public RpcRequest(String source, String destination, String method, TransmitMode transmitMode) {
        this(source, destination, method);
        setTransmitMode(transmitMode);
    }

    public RpcResponse createResponse(){
        // We flip the source and destination since we're sending it back.
        return new RpcResponse(this.getDestination(), this.getSource(), this.getMethod());
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

    public RpcRequest putArg(Class<?> key, Object value) {
        arguments.put(key, value);
        return this;
    }

    public RpcRequest putStringArg(Object value) {
        arguments.put(String.class, value);
        return this;
    }

    public RpcRequest putIntArg(Object value) {
        arguments.put(int.class, value);
        return this;
    }

    public RpcRequest putLongArg(Object value) {
        arguments.put(long.class, value);
        return this;
    }

    public RpcRequest putBoolArg(Object value) {
        arguments.put(boolean.class, value);
        return this;
    }

    public RpcRequest putResourceArg(Object value) {
        arguments.put(Resource.class, value);
        return this;
    }

    public RpcRequest putAllArg(HashMap<Class<?>, Object> extraArguments) {
        arguments.putAll(extraArguments);
        return this;
    }

    /* -- SERIALIZATION -- */
    public static RpcRequest fromByteArray(byte[] rpcRequestBytes) throws IOException {
        try {
            return (RpcRequest) Serializer.bytesToObject(rpcRequestBytes);
        } catch(ClassNotFoundException error) {
            return null;
        }
    }

    public static byte[] toByteArray(RpcRequest rpcRequest) throws IOException {
        return Serializer.objectToBytes(rpcRequest);
    }

}
