package stitch.rpc;

import stitch.util.Serializer;

import java.io.IOException;

public class RPCResponse extends RPCObject {

    private RPCStatusCode statusCode;
    private String statusMessage = "";
    private byte[] responseBytes;

    public RPCResponse(String source, String destination, String method) {
        setSource(source);
        setDestination(destination);
        setMethod(method);
    }

    /* RESPONSE STATUS CODE */
    public RPCStatusCode getStatusCode() { return statusCode; }
    public RPCResponse setStatusCode(RPCStatusCode statusCode){
        this.statusCode = statusCode;
        return this;
    }

    /* RESPONSE STATUS MESSAGE */
    public String getStatusMessage() { return statusMessage; }
    public RPCResponse setStatusMessage(String statusMessage){
        this.statusMessage = statusMessage;
        return this;
    }

    /* RESPONSE BYTES */
    public byte[] getResponseBytes(){
        return responseBytes;
    }
    public RPCResponse setResponseBytes(byte[] responseBytes){
        this.responseBytes = responseBytes;
        return this;
    }

    /* RESPONSE OBJECT */
    public Object getResponseObject() throws IOException, ClassNotFoundException{
        return Serializer.bytesToObject(getResponseBytes());
    }

    public RPCResponse setResponseObject(Object responseObject) throws IOException {
        responseBytes = Serializer.objectToBytes(responseObject);
        return this;
    }


    /* -- SERIALIZATION -- */
    public static RPCResponse fromByteArray(byte[] rpcResponseBytes) throws IOException, ClassNotFoundException {
        return (RPCResponse) Serializer.bytesToObject(rpcResponseBytes);
    }

    public static byte[] toByteArray(RPCResponse rpcResponse) throws IOException {
        return Serializer.objectToBytes(rpcResponse);
    }

}
