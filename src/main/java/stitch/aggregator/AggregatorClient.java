package stitch.aggregator;

import org.apache.log4j.Logger;
import stitch.aggregator.metastore.MetaStore;
import stitch.datastore.DataStoreInfo;
import stitch.datastore.DataStoreStatus;
import stitch.datastore.resource.ResourceRequest;
import stitch.datastore.resource.ResourceStore;
import stitch.rpc.RpcRequest;
import stitch.datastore.resource.Resource;
import stitch.transport.TransportCallableClient;
import stitch.transport.TransportFactory;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class AggregatorClient implements MetaStore, ResourceStore {

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
    public String createResource(ResourceRequest resourceRequest) throws Exception {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "createResource")
                .putArg(ResourceRequest.class, resourceRequest);
        return (String)rpcClient.invokeRPC(rpcRequest).getResponseObject();
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
    public boolean updateResource(ResourceRequest resourceRequest) throws Exception {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "findResources")
                .putArg(ResourceRequest.class, resourceRequest);
        try {
            return (boolean) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch (Exception error) {
            logger.error("Failed to find resources!", error);
            return false;
        }
    }

    @Override
    public byte[] readData(String resourceId) {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "readData")
                .putStringArg(resourceId);
        try {
            return (byte[]) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch (Exception error) {
            logger.error("Failed to read resource!", error);
            return null;
        }
    }

    @Override
    public byte[] readData(String resourceId, long offset, long length) {
        return new byte[0];
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes) {
        return 0;
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes, long offset) {
        return 0;
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
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "getDatastore")
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
    public DataStoreInfo[] listDataStores() {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "listDataStores");
        try {
            return (DataStoreInfo[]) rpcClient.invokeRPC(rpcRequest).getResponseObject();
        } catch(Exception error){
            logger.error("Failed to list datastores!", error);
            return null;
        }
    }

    @Override
    public DataStoreInfo[] findDataStores(String query) {
        return new DataStoreInfo[0];
    }

    @Override
    public void reportDataStoreStatus(DataStoreStatus dataStoreStatus) throws IOException, InterruptedException {
        RpcRequest rpcRequest = new RpcRequest("", rpcClient.getRpcAddress(), "reportDataStoreStatus")
                .putArg(DataStoreStatus.class, dataStoreStatus);
        rpcClient.broadcastRPC(rpcRequest);
    }
}
