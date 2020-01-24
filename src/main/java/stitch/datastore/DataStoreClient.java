package stitch.datastore;

import org.apache.log4j.Logger;
import stitch.rpc.RPCRequest;
import stitch.resource.Resource;
import stitch.rpc.transport.RpcAbstractClient;
import stitch.util.properties.StitchProperty;

import java.util.ArrayList;

public class DataStoreClient extends RpcAbstractClient implements DataStore {

    static final Logger logger = Logger.getLogger(DataStoreClient.class);

    public DataStoreClient(StitchProperty dataStoreProperty, StitchProperty transportProperty) throws InstantiationException, IllegalAccessException {
        super(dataStoreProperty, transportProperty);
    }

    @Override
    public String createResource(Resource resource) throws Exception  {
        RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "createResource")
                .putResourceArg(resource);
        return (String)rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public boolean updateResource(Resource resource) throws Exception {
        RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "updateResource")
                .putResourceArg(resource);
        return (boolean)rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public Resource getResource(String resourceId) throws Exception {
        RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "getResource")
                .putStringArg(resourceId);
        return (Resource) rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public boolean deleteResource(String resourceId) throws Exception {
        RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "deleteResource")
                .putStringArg(resourceId);
        return (boolean)rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public ArrayList<Resource> listResources() {
        return this.listResources(true);
    }

    @Override
    public ArrayList<Resource> listResources(boolean includeData) {
        RPCRequest rpcRequest = new RPCRequest("", rpcClientProperty.getObjectId(), "listResources")
                .putBoolArg(includeData);
        try{
            return (ArrayList<Resource>)rpcClient.invokeRPC(rpcRequest).getResponseObject();

        } catch(Exception error){
            logger.error(String.format("Failed to list the available resource metadata for datastore %s", error));
            return null;
        }
    }

    // TODO: Implement some kind of resource search logic.
    @Override
    public ArrayList<Resource> findResources(String filter) {
        return listResources();
    }

    @Override
    public void run() {
        logger.info("DatasStore client started...");
    }
}
