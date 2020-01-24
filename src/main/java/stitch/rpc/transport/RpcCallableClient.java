package stitch.rpc.transport;

import stitch.rpc.RPCRequest;
import stitch.rpc.RPCResponse;
import stitch.util.properties.StitchProperty;

import java.io.IOException;

public interface RpcCallableClient extends Runnable {
    RpcCallableClient setProperties(StitchProperty transportProperties);
    RPCResponse invokeRPC(RPCRequest rpcRequest) throws IOException, InterruptedException;
}
