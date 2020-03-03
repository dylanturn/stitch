package stitch.transport;

import stitch.rpc.RpcRequest;
import stitch.rpc.RpcResponse;

public interface TransportHandler {
    void handleBroadcastRequest(RpcRequest rpcRequest);
    RpcResponse handleRequest(RpcRequest rpcRequest);

}
