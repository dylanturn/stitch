package stitch.transport;

import stitch.rpc.RpcRequest;
import stitch.rpc.RpcResponse;

import java.io.IOException;

public interface TransportCallableClient {
    String getRpcAddress();
    RpcResponse invokeRPC(RpcRequest rpcRequest) throws IOException, InterruptedException;
    void broadcastRPC(RpcRequest rpcRequest) throws IOException, InterruptedException;
    boolean isReady();
    boolean isConnected();
}
