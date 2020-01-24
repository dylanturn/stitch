package stitch.rpc.transport;


import stitch.util.properties.StitchProperty;

public interface RpcCallableServer extends Runnable {
    RpcCallableServer setProperties(StitchProperty transportProperties);
    RpcCallableServer setParent(String serverType, String serverId);
    RpcCallableServer setRpcHandler(RpcRequestHandler rpcRequestHandler);
}
