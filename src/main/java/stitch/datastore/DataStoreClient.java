package stitch.datastore;

import org.apache.log4j.Logger;
import stitch.amqp.AMQPClient;
import stitch.amqp.AMQPPrefix;
import stitch.amqp.rpc.RPCRequest;
import stitch.resource.Resource;

import java.util.ArrayList;

public class DataStoreClient extends AMQPClient implements DataStore {

    static final Logger logger = Logger.getLogger(DataStoreClient.class);


    public DataStoreClient(String id) throws Exception {
        super(AMQPPrefix.DATASTORE, id);
    }

    @Override
    public String createResource(Resource resource) throws Exception  {
        RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "createResource")
                .putArg("resource", resource);
        return (String)invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public boolean updateResource(Resource resource) throws Exception {
        RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "updateResource")
                .putArg("resource", resource);
        return (boolean)invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public Resource getResource(String resourceId) throws Exception {
        RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "getResource")
                .putArg("resourceId", resourceId);
        return (Resource) invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public boolean deleteResource(String resourceId) throws Exception {
        RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "deleteResource")
                .putArg("resourceId", resourceId);
        return (boolean)invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public ArrayList<Resource> listResources() {
        return this.listResources(true);
    }

    @Override
    public ArrayList<Resource> listResources(boolean includeData) {
        RPCRequest rpcRequest = new RPCRequest("", getRouteKey(), "listResources")
                .putArg("includeData", includeData);
        try{
            return (ArrayList<Resource>)invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error(String.format("Failed to list the available resource metadata for datastore %s", getId()),error);
            return null;
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
