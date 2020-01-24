package stitch.rpc.transport;

import stitch.util.properties.StitchProperty;

public class RpcTransportFactory {

    public static RpcCallableClient newRpcClient(StitchProperty transportProperties) throws IllegalAccessException, InstantiationException {
        RpcCallableClient rpcClient = (RpcCallableClient)transportProperties.getObjectClass().newInstance();
        return rpcClient.setProperties(transportProperties);
    }

    public static RpcCallableServer newRpcServer(StitchProperty stitchProperty, StitchProperty transportProperties) throws IllegalAccessException, InstantiationException {
        RpcCallableServer rpcServer = (RpcCallableServer)transportProperties.getObjectClass().newInstance();
        return rpcServer
                .setParent(stitchProperty.getObjectType().toString(), stitchProperty.getObjectId())
                .setProperties(transportProperties);
    }

}
