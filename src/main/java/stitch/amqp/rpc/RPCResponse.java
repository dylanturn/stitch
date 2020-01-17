package stitch.amqp.rpc;

import stitch.util.Serializer;

import java.io.IOException;

public class RPCResponse extends RPCObject {

    private byte[] responseBytes;

    public RPCResponse(RPCRequest rpcRequest) {
        super(rpcRequest.getDestination(), rpcRequest.getSource(), rpcRequest.getMethod());
    }

    public RPCResponse setResponseBytes(byte[] responseBytes){
        this.responseBytes = responseBytes;
        return this;
    }

    public RPCResponse setResponseObject(Object responseObject){
        return setResponseObject(responseObject);
    }

    public int getResponseLength(){
        return getResponseBytes().length;
    }

    public byte[] getResponseBytes(){
        return responseBytes;
    }

    public Object getResponseObject() throws IOException, ClassNotFoundException{
        return Serializer.bytesToObject(getResponseBytes());
    }

}
