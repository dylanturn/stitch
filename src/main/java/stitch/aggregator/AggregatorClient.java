package stitch.aggregator;

import org.apache.log4j.Logger;
import stitch.rpc.transport.RpcAbstractClient;
import stitch.util.properties.StitchProperty;
import stitch.rpc.transport.metrics.RpcEndpointReport;
import stitch.rpc.RPCRequest;
import stitch.resource.Resource;

import java.util.ArrayList;

public class AggregatorClient extends RpcAbstractClient implements Aggregator {

    static final Logger logger = Logger.getLogger(AggregatorClient.class);

    public AggregatorClient(StitchProperty aggregatorProperty, StitchProperty transportProperty) throws InstantiationException, IllegalAccessException {
        super(aggregatorProperty, transportProperty);
    }

    @Override
    public String createResource(Resource resource) {
        return null;
    }

    @Override
    public boolean updateResource(Resource resource) {
        RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "updateResource")
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
        RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "getResource")
               .putStringArg(resourceId);
        try {
            return (Resource) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){}
        return null;
    }

    @Override
    public boolean deleteResource(String resourceId) {
        RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "deleteResource")
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
        RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "findResources")
                .putStringArg(filter);
        try {
            return (ArrayList<Resource>) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch (Exception error) {
            logger.error("Failed to find resources!", error);
            return null;
        }
    }

    @Override
    public ArrayList<Resource> listResources() {
        RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "listResources");
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
            RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "listDataStores");
            return (ArrayList<RpcEndpointReport>) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to list resources!", error);
            return null;
        }
    }

    @Override
    public void registerResource(String datastoreId, Resource resource) {
        RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "registerResource")
                .putStringArg(datastoreId)
                .putResourceArg(resource);
        try {
            logger.info(String.format("Registering resource: %s", resource.getUUID()));
            logger.info(String.format("Datastore Id:         %s", datastoreId));
            logger.info(String.format("Route Key:            %s", rpcClientProperty.getObjectId()));
            rpcClient.invokeRPC(rpcRequest);
        } catch(Exception error){
            logger.error("Failed to register resource!", error);
        }
    }

    @Override
    public void run() {
        logger.info("Started Aggregator Client...");
    }
}
