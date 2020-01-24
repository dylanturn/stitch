package stitch.rpc.transport;

import stitch.util.properties.StitchProperty;

public abstract class RpcAbstractClient implements Runnable {

    protected StitchProperty rpcClientProperty;
    protected StitchProperty transportProperty;
    protected RpcCallableClient rpcClient;

    public RpcAbstractClient(StitchProperty rpcClientProperty, StitchProperty transportProperty) throws InstantiationException, IllegalAccessException {
        this.rpcClientProperty = rpcClientProperty;
        this.transportProperty = transportProperty;
        rpcClient = RpcTransportFactory.newRpcClient(transportProperty);
    }
}
