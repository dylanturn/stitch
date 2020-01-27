package stitch.rpc.transport;

import stitch.rpc.metrics.RpcEndpointReport;

public interface RpcCallableServer extends Runnable {
    String getRpcAddress();
    RpcEndpointReport generateEndpointReport();
}
