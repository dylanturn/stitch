package stitch.transport;

import stitch.rpc.RpcRequest;
import stitch.rpc.RpcResponse;

public interface TransportHandler {
    RpcResponse handleRequest(RpcRequest rpcRequest);
}
