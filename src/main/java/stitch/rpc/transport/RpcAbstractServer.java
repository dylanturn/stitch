package stitch.rpc.transport;

import stitch.util.properties.StitchProperty;

public abstract class RpcAbstractServer implements Runnable {

    protected StitchProperty rpcServerProperty;
    protected StitchProperty transportProperty;
    protected RpcCallableServer rpcServer;

    public RpcAbstractServer(StitchProperty rpcServerProperty, StitchProperty transportProperty) throws InstantiationException, IllegalAccessException {
        this.rpcServerProperty = rpcServerProperty;
        this.transportProperty = transportProperty;
        rpcServer = RpcTransportFactory.newRpcServer(rpcServerProperty, transportProperty);
    }
}
