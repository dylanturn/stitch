package stitch.rpc.transport;

import stitch.aggregator.Aggregator;
import stitch.datastore.DataStore;
import stitch.rpc.RPCRequest;
import stitch.rpc.RPCResponse;
import stitch.rpc.RPCStatusCode;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class RpcRequestHandler {

    Class<?> serverClass;
    Object serverObject;

    public RpcRequestHandler(Aggregator aggregator) {
        this.serverClass = aggregator.getClass();
        this.serverObject = aggregator;
    }

    public RpcRequestHandler(DataStore dataStore) {
        this.serverClass = dataStore.getClass();
        this.serverObject = dataStore.getClass();
    }

    public RPCResponse handleRequest(RPCRequest rpcRequest) {

        String methodName = rpcRequest.getMethod();
        Class<?>[] methodArgClasses = rpcRequest.getArgClasses();
        Object[] methodArgValues = rpcRequest.getArgValues();

        try {

            Method method = serverClass.getMethod(methodName, methodArgClasses);
            return rpcRequest.createResponse()
                    .setStatusCode(RPCStatusCode.OK)
                    .setResponseObject(method.invoke(serverObject, methodArgValues));

        } catch (NoSuchMethodException error) {
            return rpcRequest.createResponse()
                    .setStatusCode(RPCStatusCode.NOT_IMPLEMENTED)
                    .setStatusMessage(error.getMessage());
        } catch (IllegalAccessException error) {
            return rpcRequest.createResponse()
                    .setStatusCode(RPCStatusCode.SERVER_ERROR)
                    .setStatusMessage(error.getMessage());
        } catch (InvocationTargetException error) {
            return rpcRequest.createResponse()
                    .setStatusCode(RPCStatusCode.SERVER_ERROR)
                    .setStatusMessage(error.getMessage());
        } catch (IOException error) {
            return rpcRequest.createResponse()
                    .setStatusCode(RPCStatusCode.NET_READ_ERROR)
                    .setStatusMessage(error.getMessage());
        }
    }
}
