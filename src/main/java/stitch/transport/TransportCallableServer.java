package stitch.transport;

import stitch.transport.metrics.RpcEndpointReporter;

public interface TransportCallableServer extends Runnable {
    RpcEndpointReporter getRpcEndpointReporter();
}
