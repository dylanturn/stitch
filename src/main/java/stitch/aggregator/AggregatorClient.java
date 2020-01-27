package stitch.aggregator;

import org.apache.log4j.Logger;
import stitch.rpc.RPCResponse;
import stitch.rpc.RPCStatusCode;
import stitch.rpc.metrics.RpcEndpointReport;
import stitch.rpc.RPCRequest;
import stitch.resource.Resource;
import stitch.rpc.transport.RpcCallableClient;
import stitch.rpc.transport.RpcTransportFactory;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class AggregatorClient implements Aggregator {

    static final Logger logger = Logger.getLogger(AggregatorClient.class);

    protected ConfigItem endpointConfig;
    protected RpcCallableClient rpcClient;

    public AggregatorClient(String endpointId) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        rpcClient = RpcTransportFactory.newRpcClient(endpointId);
    }

    public boolean isRpcReady(){
        return rpcClient.isReady();
    }
    public boolean isRpcConnected(){
        return rpcClient.isConnected();
    }

    @Override
    public String createResource(Resource resource) {
        return null;
    }

    @Override
    public boolean updateResource(Resource resource) {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "updateResource")
                .putResourceArg(resource);
        try {
            return (boolean) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to update the resource: " + resource.getUUID(), error);
            return false;
        }
    }

    @Override
    public Resource getResource(String resourceId) {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "getResource")
               .putStringArg(resourceId);
        try {
            return (Resource) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){}
        return null;
    }

    @Override
    public boolean deleteResource(String resourceId) {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "deleteResource")
                .putStringArg(resourceId);
        try {
            return (boolean) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to delete resources!", error);
            return false;
        }
    }

    @Override
    public ArrayList<Resource> findResources(String filter) {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "findResources")
                .putStringArg(filter);
        try {
            return (ArrayList<Resource>) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch (Exception error) {
            logger.error("Failed to find resources!", error);
            return null;
        }
    }

    @Override
    public RpcEndpointReport getEndpointReport() throws IOException, InterruptedException, ClassNotFoundException {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "generateEndpointReport");
        return (RpcEndpointReport)rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public ArrayList<Resource> listResources() {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "listResources");
        try {
            return (ArrayList<Resource>) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to list resources!", error);
            return null;
        }
    }

    // TODO: This should work the way it implies.
    @Override
    public Iterable<Resource> listResources(boolean includeData) {
        return listResources();
    }

    @Override
    public ArrayList<RpcEndpointReport> listDataStores() {
        try {
            RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "listDataStores");
            RPCResponse rpcResponse = rpcClient.invokeRPC(rpcRequest);

            if(rpcResponse.getStatusCode() == RPCStatusCode.OK){
                return (ArrayList<RpcEndpointReport>)rpcResponse.getResponseObject();
            } else {
                logger.warn("Response Code:    " + rpcResponse.getStatusCode());
                logger.warn("Response Message: " + rpcResponse.getStatusMessage());
                return null;
            }
        } catch(Exception error){
            logger.error("Failed to list resources!", error);
            return null;
        }
    }

    @Override
    public void registerResource(String datastoreId, Resource resource) {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "registerResource")
                .putStringArg(datastoreId)
                .putResourceArg(resource);
        try {
            logger.info(String.format("Registering resource: %s", resource.getUUID()));
            logger.info(String.format("Datastore Id:         %s", datastoreId));
            logger.info(String.format("Route Key:            %s", rpcClient.getRpcAddress()));
            rpcClient.invokeRPC(rpcRequest);
        } catch(Exception error){
            logger.error("Failed to register resource!", error);
        }
    }
}
