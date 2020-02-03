package stitch.datastore;

import org.apache.log4j.Logger;
import stitch.resource.ResourceCallable;
import stitch.rpc.RpcRequest;
import stitch.resource.Resource;
import stitch.transport.TransportCallableClient;
import stitch.transport.TransportFactory;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class DataStoreClient implements DataStoreCallable, ResourceCallable {

    static final Logger logger = Logger.getLogger(DataStoreClient.class);

    protected ConfigItem endpointConfig;
    protected TransportCallableClient rpcClient;
    protected DataStoreReport peerEndpointReport;

    public DataStoreClient(String endpointId) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        rpcClient = TransportFactory.newRpcClient(endpointId);
    }

    @Override
    public String createResource(Resource resource) throws Exception  {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "createResource")
                .putResourceArg(resource);
        return (String)rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public boolean updateResource(Resource resource) throws Exception {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "updateResource")
                .putResourceArg(resource);
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
        return this.listResources(true);
    }

    @Override
    public ArrayList<Resource> listResources(boolean includeData) {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "listResources")
                .putBoolArg(includeData);
        try{
            return (ArrayList<Resource>)rpcClient.invokeRPC(rpcRequest).getResponseObject();

        } catch(Exception error){
            logger.error(String.format("Failed to list the available resource metadata for datastore %s", endpointConfig.getConfigName()), error);
            return null;
        }
    }

    // TODO: Implement some kind of resource search logic.
    @Override
    public ArrayList<Resource> findResources(String filter) {
        return listResources();
    }


    @Override
    public DataStoreReport getEndpointReport() throws IOException, InterruptedException, ClassNotFoundException {
        return peerEndpointReport;
    }

    @Override
    public long getResourceCount() {
        return peerEndpointReport.getResourceCount();
    }

    @Override
    public long getUsedStorage() {
        return peerEndpointReport.getUsedStorageSpace();
    }
}
