package stitch.datastore;

import org.apache.log4j.Logger;
import org.bson.Document;
import stitch.amqp.AMQPHandler;
import stitch.amqp.AMQPPrefix;
import stitch.amqp.rpc.RPCRequest;
import stitch.amqp.rpc.RPCResponse;
import stitch.amqp.rpc.RPCStatusCode;
import stitch.resource.Resource;
import stitch.amqp.AMQPServer;

public abstract class DataStoreServer extends AMQPServer implements DataStore {

    static final Logger logger = Logger.getLogger(DataStoreServer.class);

    protected Document providerArgs;

    public DataStoreServer(Document providerArgs) throws Exception {
        super(AMQPPrefix.DATASTORE, providerArgs.getString("uuid"));

        this.providerArgs = providerArgs;
        this.addMetaData("type", providerArgs.getString("type"));
        this.addMetaData("class", providerArgs.getString("class"));

        // TODO: Error handling here needs to be improved.
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
                            return rpcRequest.createResponse()
                                    .setStatusCode(RPCStatusCode.OK)
                                    .setResponseObject(listResources());
                        } catch(Exception error){
                            logger.error("Failed to list resources", error);
                            return  rpcRequest.createResponse()
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
    public void run() {
        this.connect();
        this.consumeAMQP();
    }

    public abstract void connect();

    public String getStoreType(){
        return this.getMetaString("type");
    }

    public String getStoreClass(){
        return this.getMetaString("class");
    }
}
