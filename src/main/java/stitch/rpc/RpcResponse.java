package stitch.rpc;

import stitch.util.Serializer;

import java.io.IOException;

public class RpcResponse extends Rpc {

    private RpcStatusCode statusCode;
    private String statusMessage = "";
    private byte[] responseBytes;

    public RpcResponse(String source, String destination, String method) {
        setSource(source);
        setDestination(destination);
        setMethod(method);
    }

    /* RESPONSE STATUS CODE */
    public RpcStatusCode getStatusCode() { return statusCode; }
    public RpcResponse setStatusCode(RpcStatusCode statusCode){
        this.statusCode = statusCode;
        return this;
    }

    /* RESPONSE STATUS MESSAGE */
    public String getStatusMessage() { return statusMessage; }
    public RpcResponse setStatusMessage(String statusMessage){
        this.statusMessage = statusMessage;
        return this;
    }

    /* RESPONSE BYTES */
    public byte[] getResponseBytes(){
        return responseBytes;
    }
    public RpcResponse setResponseBytes(byte[] responseBytes){
        this.responseBytes = responseBytes;
        return this;
    }

    /* RESPONSE OBJECT */
    public Object getResponseObject() throws IOException, ClassNotFoundException{
        return Serializer.bytesToObject(getResponseBytes());
    }

    public RpcResponse setResponseObject(Object responseObject) throws IOException {
        responseBytes = Serializer.objectToBytes(responseObject);
        return this;
    }


    /* -- SERIALIZATION -- */
    public static RpcResponse fromByteArray(byte[] rpcResponseBytes) throws IOException, ClassNotFoundException {
        return (RpcResponse) Serializer.bytesToObject(rpcResponseBytes);
    }

    public static byte[] toByteArray(RpcResponse rpcResponse) throws IOException {
        return Serializer.objectToBytes(rpcResponse);
    }

}
