package stitch.aggregator;

import org.apache.log4j.Logger;
import stitch.aggregator.metastore.MetaStoreCallable;
import stitch.datastore.DataStoreInfo;
import stitch.datastore.DataStoreStatus;
import stitch.datastore.ReplicaStatus;
import stitch.resource.ResourceCallable;
import stitch.resource.ResourceStatus;
import stitch.rpc.RpcRequest;
import stitch.resource.Resource;
import stitch.transport.TransportCallableClient;
import stitch.transport.TransportFactory;
import stitch.util.EndpointStatus;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class AggregatorClient implements MetaStoreCallable, ResourceCallable {

    static final Logger logger = Logger.getLogger(AggregatorClient.class);

    protected ConfigItem endpointConfig;
    protected TransportCallableClient rpcClient;

    public AggregatorClient(String endpointId) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        rpcClient = TransportFactory.newRpcClient(endpointId);
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
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "updateResource")
                .putResourceArg(resource);
        try {
            return (boolean) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to update the resource: " + resource.getID(), error);
            return false;
        }
    }

    @Override
    public Resource getResource(String resourceId) {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "getResource")
               .putStringArg(resourceId);
        try {
            return (Resource) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to get resource!", error);
        }
        return null;
    }

    @Override
    public boolean deleteResource(String resourceId) {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "deleteResource")
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
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "findResources")
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
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "listResources");
        try {
            return (ArrayList<Resource>) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to list resources!", error);
            return null;
        }
    }


    @Override
    public DataStoreInfo getDatastore(String dataStoreId) {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "listDataStores")
                .putArg(String.class, dataStoreId);
        try {
            return (DataStoreInfo) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error) {
            logger.error("Failed to list datastores!", error);
            return null;
        }
    }

    // We should shy away from returning array lists. Lets return an object array.
    @Override
    public ArrayList<DataStoreInfo> listDataStores() {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "listDataStores");
        try {
            return (ArrayList<DataStoreInfo>) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to list datastores!", error);
            return null;
        }
    }

    @Override
    public DataStoreInfo[] findDataStores() {
        return listDataStores().toArray(new DataStoreInfo[0]);
    }

    @Override
    public void reportDataStoreStatus(DataStoreStatus dataStoreStatus) throws IOException, InterruptedException {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "reportDataStoreStatus")
                .putArg(DataStoreStatus.class, dataStoreStatus);
        rpcClient.broadcastRPC(rpcRequest);
    }
}
