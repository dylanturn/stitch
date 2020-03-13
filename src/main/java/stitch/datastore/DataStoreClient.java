package stitch.datastore;

import org.apache.log4j.Logger;
import stitch.datastore.query.SearchQuery;
import stitch.datastore.resource.ResourceRequest;
import stitch.datastore.resource.ResourceStore;
import stitch.rpc.RpcRequest;
import stitch.datastore.resource.Resource;
import stitch.transport.TransportCallableClient;
import stitch.transport.TransportFactory;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class DataStoreClient implements ResourceStore {

    static final Logger logger = Logger.getLogger(DataStoreClient.class);

    protected ConfigItem endpointConfig;
    protected TransportCallableClient rpcClient;

    public DataStoreClient(String endpointId) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        this(ConfigStore.loadConfigStore().getConfigItemById(endpointId));
    }

    public DataStoreClient(ConfigItem endpointConfig) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        this.endpointConfig = endpointConfig;
        rpcClient = TransportFactory.newRpcClient(endpointConfig.getConfigId());
    }

    @Override
    public String createResource(ResourceRequest resourceRequest) throws Exception {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "createResource")
                .putArg(ResourceRequest.class, resourceRequest);
        return (String)rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public boolean updateResource(ResourceRequest resourceRequest) throws Exception {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "updateResource")
                .putStringArg(resourceRequest.getId())
                .putArg(ResourceRequest.class, resourceRequest);
        return (boolean)rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public Resource getResource(String resourceId) throws Exception {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "getResource")
                .putStringArg(resourceId);
        return (Resource) rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public boolean deleteResource(String resourceId) throws Exception {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "deleteResource")
                .putStringArg(resourceId);
        return (boolean)rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public ArrayList<Resource> listResources() {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "listResources");
        try{
            return (ArrayList<Resource>)rpcClient.invokeRPC(rpcRequest).getResponseObject();

        } catch(Exception error){
            logger.error(String.format("Failed to list the available resource metadata for datastore %s", endpointConfig.getConfigName()), error);
            return null;
        }
    }

    // TODO: Implement some kind of resource search logic.
    @Override
    public ArrayList<Resource> findResources(SearchQuery query) {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "findResources")
                .putArg(SearchQuery.class, query);
        try{
            return (ArrayList<Resource>)rpcClient.invokeRPC(rpcRequest).getResponseObject();

        } catch(Exception error){
            logger.error(String.format("Failed to list the available resource metadata for datastore %s", endpointConfig.getConfigName()), error);
            return null;
        }
    }

    @Override
    public byte[] readData(String resourceId) {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "readData")
                .putStringArg(resourceId);
        try{
            return (byte[])rpcClient.invokeRPC(rpcRequest).getResponseObject();

        } catch(Exception error){
            logger.error(String.format("Failed to read data from %s", resourceId), error);
            return null;
        }
    }

    @Override
    public byte[] readData(String resourceId, long offset, long length) {
        return new byte[0];
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes) {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "writeData")
                .putStringArg(resourceId)
                .putArg(byte[].class, dataBytes);
        try{
            return (int)rpcClient.invokeRPC(rpcRequest).getResponseObject();

        } catch(Exception error){
            logger.error(String.format("Failed to read data from %s", resourceId), error);
            return -1;
        }
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes, long offset) {
        return 0;
    }
}
