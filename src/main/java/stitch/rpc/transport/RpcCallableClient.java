package stitch.rpc.transport;

import stitch.rpc.RPCRequest;
import stitch.rpc.RPCResponse;

import java.io.IOException;

public interface RpcCallableClient {
    String getRpcAddress();
    RPCResponse invokeRPC(RPCRequest rpcRequest) throws IOException, InterruptedException;
    boolean isReady();
    boolean isConnected();
}
