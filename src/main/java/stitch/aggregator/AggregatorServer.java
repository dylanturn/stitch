package stitch.aggregator;

import org.apache.log4j.Logger;
import org.bson.Document;
import stitch.amqp.AMQPHandler;
import stitch.amqp.AMQPServer;
import stitch.amqp.HealthReport;
import stitch.amqp.AMQPPrefix;
import stitch.amqp.rpc.RPCRequest;
import stitch.amqp.rpc.RPCResponse;
import stitch.amqp.rpc.RPCStatusCode;
import stitch.datastore.DataStoreClient;
import stitch.resource.Resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class AggregatorServer extends AMQPServer implements Aggregator, Runnable {

    static final Logger logger = Logger.getLogger(AggregatorServer.class);

    protected Document aggregatorArgs;
    protected HashMap<String, DataStoreClient> providerClients = new HashMap<>();

    public AggregatorServer(Document aggregatorArgs, Iterable<Document> providers) throws Exception {
        super(AMQPPrefix.AGGREGATOR, aggregatorArgs.getString("uuid"));
        this.aggregatorArgs = aggregatorArgs;

        for(Document provider : providers){
            String providerUUID = provider.getString("uuid");
            providerClients.put(providerUUID, new DataStoreClient(providerUUID));
        }

        setHandler(new AMQPHandler(this) {
            @Override
            protected RPCResponse routeRPC(RPCRequest rpcRequest) {

                switch (rpcRequest.getMethod()) {

                    case "createResource":
                        try {
                            logger.trace("Received request to create new resource");
                            Resource resource = (Resource)rpcRequest.getArg("resource");
                            logger.trace("Creating new resource with UUID: " + resource.getUUID());
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.OK)
                                    .setResponseObject(createResource(resource));
                        } catch(Exception error){
                            logger.error("Failed to create resource!", error);
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.ERROR)
                                    .setStatusMessage(error.getMessage());
                        }

                    case "updateResource":
                        try {
                            logger.trace("Received request to update new resource");
                            Resource resource = (Resource)rpcRequest.getArg("resource");
                            logger.trace("Updating resource with UUID: " + resource.getUUID());
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.OK)
                                    .setResponseObject(updateResource(resource));
                        } catch (Exception error){
                            logger.error("Failed to update resource!", error);
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.ERROR)
                                    .setStatusMessage(error.getMessage());
                        }

                    case "getResource":
                        try {
                            logger.info("Received request to get a resource");
                            String resourceId = rpcRequest.getStringArg("resourceId");
                            logger.info("Getting a resource with UUID: " + resourceId);
                            Resource resource = getResource(resourceId);
                            if(resource == null) {
                                logger.warn(String.format("Resource %s not found.", resourceId));
                                return rpcRequest.createResponse()
                                        .setStatusCode(RPCStatusCode.MISSING);
                            }
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.OK)
                                    .setResponseObject(resource);
                        } catch ( Exception error) {
                            logger.error("Failed to get resource", error);
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.ERROR)
                                    .setStatusMessage(error.getMessage());
                        }

                    case "deleteResource":
                        try {
                            logger.info("Received request to delete a resource");
                            String resourceId = rpcRequest.getStringArg("resourceId");
                            if(deleteResource(resourceId)) {
                                return rpcRequest.createResponse()
                                        .setStatusCode(RPCStatusCode.OK);
                            } else {
                                logger.warn(String.format("Resource %s not found.", resourceId));
                                return rpcRequest.createResponse()
                                        .setStatusCode(RPCStatusCode.MISSING)
                                        .setStatusMessage("Resource not found.");
                            }
                        } catch ( Exception error) {
                            logger.error(String.format("Failed to delete Resource", error));
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.ERROR)
                                    .setStatusMessage(error.getMessage());
                        }

                    case "listResources":
                        try {
                            logger.info("Received request to list resources");
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.OK)
                                    .setResponseObject(listResources());
                        } catch(Exception error){
                            logger.error("Failed to list resources", error);
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.ERROR)
                                    .setStatusMessage(error.getMessage());
                        }

                    case "listDataStores":
                        try{
                            logger.info("Received request to list the datastores");
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.OK)
                                    .setResponseObject(listDataStores());
                        } catch(Exception error){
                            logger.error("Failed to list datastores.", error);
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.ERROR)
                                    .setStatusMessage(error.getMessage());
                        }

                    case "findResources":
                        try {
                            logger.info("Received request to find a resource");
                            String filterString = rpcRequest.getStringArg("filter");
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.OK)
                                    .setResponseObject(findResources(filterString));
                        } catch (Exception error){
                            logger.error("Failed to find resource!", error);
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.ERROR)
                                    .setStatusMessage(error.getMessage());
                        }

                    case "registerResource":
                        try {
                            logger.info("Received request to register a resource");
                            String datastoreId = rpcRequest.getStringArg("datastoreId");
                            Resource resource = (Resource)rpcRequest.getArg("resource");

                            logger.trace(String.format("Caller ID:     %s", datastoreId));
                            logger.trace(String.format("Resource ID:   %s", resource.getUUID()));
                            registerResource(datastoreId, resource);

                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.OK);

                        } catch (Exception error){
                            logger.error("Failed to register resource!", error);
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.ERROR)
                                    .setStatusMessage(error.getMessage());
                        }

                    default:
                        logger.error("Failed to match RPC method: " + rpcRequest.getMethod());
                        return rpcRequest.createResponse()
                                .setStatusCode(RPCStatusCode.MISSING)
                                .setStatusMessage("Failed to match RPC method: " + rpcRequest.getMethod());
                }

            }
        });
        new Thread(this).start();
    }

    @Override
    public ArrayList<HealthReport> listDataStores() {
        ArrayList<HealthReport> dataStoreHealthReports = new ArrayList<>();
        for(DataStoreClient dataStoreClient : providerClients.values()){
            dataStoreHealthReports.add(dataStoreClient.getLastHealthReport());
        }
        return dataStoreHealthReports;
    }

    private void registerResources(){
        logger.info("Registering datastore resources!");
        for(Map.Entry<String, DataStoreClient> dataStoreClient : providerClients.entrySet()){
            logger.info("Registering datastore resources.");
            ArrayList<Resource> resourceArray = dataStoreClient.getValue().listResources();
            for(Resource resource : resourceArray) {
                this.registerResource(dataStoreClient.getKey(), resource);
            }
        }
    }

    @Override
    public void run() {
        // Make sure the Aggregator has connected to the cache
        this.connect();
        // Begin consuming from the Aggregators' queue
        this.consumeAMQP();
        // Request a list of resources from all the DataStores and put them in the cache.
        this.registerResources();
    }


    public abstract void connect();
}
