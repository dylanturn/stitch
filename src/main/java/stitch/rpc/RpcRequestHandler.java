package stitch.rpc;

import org.apache.log4j.Logger;
import stitch.datastore.DataStoreCallable;
import stitch.transport.TransportHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class RpcRequestHandler implements TransportHandler {

    private static final Logger logger = Logger.getLogger(RpcRequestHandler.class);

    Class<?> serverClass;
    Object serverObject;

    public RpcRequestHandler(Class serverClass, Object serverObject){
        this.serverClass = serverClass;
        this.serverObject = serverObject;
    }

    public RpcRequestHandler(DataStoreCallable dataStore) {

        this.serverClass = dataStore.getClass();
        this.serverObject = dataStore;
    }

    @Override
    public void handleBroadcastRequest(RpcRequest rpcRequest) {
        // do this
    }

    @Override
    public RpcResponse handleRequest(RpcRequest rpcRequest) {

        String methodName = rpcRequest.getMethod();
        Class<?>[] methodArgClasses = rpcRequest.getArgClasses();
        Object[] methodArgValues = rpcRequest.getArgValues();

        try {

            Method method = serverClass.getMethod(methodName, methodArgClasses);
            Object responseObject = method.invoke(serverObject, methodArgValues);
            if(responseObject != null){
                // We return 200 because the call returned an object without throwing an error.
                return rpcRequest.createResponse()
                        .setStatusCode(RpcStatusCode.OK)
                        .setResponseObject(responseObject);
            } else {
                // We return 204 because the responseObject is null byt we didn't throw an error.
                return rpcRequest.createResponse()
                        .setStatusCode(RpcStatusCode.EMPTY);
            }

        } catch (NoSuchMethodException error) {
            logger.error(error);
            return rpcRequest.createResponse()
                    .setStatusCode(RpcStatusCode.NOT_IMPLEMENTED)
                    .setStatusMessage(error.getMessage());
        } catch (IllegalAccessException error) {
            logger.error("Caught IllegalAccessException", error);
            return rpcRequest.createResponse()
                    .setStatusCode(RpcStatusCode.SERVER_ERROR)
                    .setStatusMessage(error.getMessage());
        } catch (InvocationTargetException error) {
            logger.error("Failed to invoke RPC", error);
            return rpcRequest.createResponse()
                    .setStatusCode(RpcStatusCode.SERVER_ERROR)
                    .setStatusMessage(error.getMessage());
        } catch (IOException error) {
            logger.error(error);
            return rpcRequest.createResponse()
                    .setStatusCode(RpcStatusCode.NET_READ_ERROR)
                    .setStatusMessage(error.getMessage());
        } finally {
            // Gross, i know.
            StringBuilder traceBuilder = new StringBuilder();
            traceBuilder
                    .append("RPC INFO").append("\n")
                    .append("RPC Method:            " + rpcRequest.getMethod()).append("\n")
                    .append("RPC Source:            " + rpcRequest.getSource()).append("\n")
                    .append("RPC Destination:       " + rpcRequest.getDestination()).append("\n")
                    .append("RPC CLASSES").append("\n");
            for(Class rpcClass : rpcRequest.getArgClasses()){
                traceBuilder.append(" - RPC Class Name: " + rpcClass.getName()).append("\n");
            }
            traceBuilder.append("RPC CLASS ARGS").append("\n");
            for(Object rpcObject : rpcRequest.getArgValues()){
                traceBuilder.append(" - RPC Class Arg: " + rpcObject.getClass().getName()).append("\n");
            }
            logger.trace(traceBuilder.toString());
        }
    }
}
