package stitch.aggregator;

import org.apache.log4j.Logger;
import stitch.amqp.AMQPClient;
import stitch.amqp.AMQPPrefix;
import stitch.amqp.HealthReport;
import stitch.amqp.rpc.RPCRequest;
import stitch.resource.Resource;

import java.util.ArrayList;

public class AggregatorClient extends AMQPClient implements Aggregator {

    static final Logger logger = Logger.getLogger(AggregatorClient.class);

    public AggregatorClient(String id) throws Exception {
        super(AMQPPrefix.AGGREGATOR, id);
    }

    @Override
    public String createResource(Resource resource) {
        return null;
    }

    @Override
    public boolean updateResource(Resource resource) {
        RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "updateResource")
                .putArg("resource", resource);
        try {
            return (boolean) invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to update the resource: " + resource.getUUID(), error);
            return false;
        }
    }

    @Override
    public Resource getResource(String resourceId) {
        RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "getResource")
                .putArg("resourceId", resourceId);
        try {
            return (Resource)invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){}
        return null;
    }

    @Override
    public boolean deleteResource(String resourceId) {
        RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "deleteResource")
                .putArg("resourceId", resourceId);
        try {
            return (boolean) invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to delete resources!", error);
            return false;
        }
    }

    @Override
    public ArrayList<Resource> findResources(String filter) {
        RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "findResources")
                .putArg("filter", filter);
        try {
            return (ArrayList<Resource>) invokeRPC(rpcRequest).getResponseObject();
        } catch (Exception error) {
            logger.error("Failed to find resources!", error);
            return null;
        }
    }

    @Override
    public ArrayList<Resource> listResources() {
        RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "listResources");
        try {
            return (ArrayList<Resource>)invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to list resources!", error);
            return null;
        }
    }

    @Override
    public ArrayList<HealthReport> listDataStores() {
        try {
            RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "listDataStores");
            return (ArrayList<HealthReport>)invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to list resources!", error);
            return null;
        }
    }

    @Override
    public void registerResource(String datastoreId, Resource resource) {
        RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "registerResource")
                .putArg("datastoreId", datastoreId)
                .putArg("resource", resource);
        try {
            logger.info(String.format("Registering resource: %s", resource.getUUID()));
            logger.info(String.format("Datastore Id:         %s", datastoreId));
            logger.info(String.format("Route Key:            %s", getRouteKey()));
            invokeRPC(rpcRequest);
        } catch(Exception error){
            logger.error("Failed to register resource!", error);
        }
    }

    @Override
    public void run() {
        logger.info("Started Aggregator Client...");
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down aggregator client...");
    }
}
