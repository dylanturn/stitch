package stitch.aggregator;

import org.apache.log4j.Logger;
import stitch.aggregator.metastore.MetaStore;
import stitch.datastore.DataStoreClient;
import stitch.resource.ResourceRequest;
import stitch.resource.ResourceStore;
import stitch.transport.TransportCallableServer;
import stitch.rpc.RpcRequestHandler;
import stitch.transport.TransportFactory;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.item.ConfigItemType;
import stitch.util.configuration.store.ConfigStore;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AggregatorServer implements ResourceStore, Runnable {

    static final Logger logger = Logger.getLogger(AggregatorServer.class);

    private ConfigStore configStore;
    protected ConfigItem endpointConfig;
    protected TransportCallableServer rpcServer;
    protected HashMap<String, DataStoreClient> dataStoreClients = new HashMap<>();
    private MetaStore callableMetaStore;
    private AggregatorAPI aggregatorAPI;

    public AggregatorServer(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        configStore = ConfigStore.loadConfigStore();
        this.endpointConfig = endpointConfig;
    }

    private void createCallableAggregator() throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        Class<? extends MetaStore> metaStoreCallableClass = endpointConfig.getConfigClass("class");
        Constructor<?> metaStoreCallableClassConstructor = metaStoreCallableClass.getConstructor(AggregatorServer.class);
        this.callableMetaStore = (MetaStore) metaStoreCallableClassConstructor.newInstance(this);
    }

    private void connectDataStoreClients() throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        // Create a Map that holds all the filters we'll use to get the aggregators datastores.
        Map<String, String> filters = new HashMap<>();
        filters.put("type", ConfigItemType.DATASTORE.toString());
        filters.put("aggregator", endpointConfig.getConfigId());

        for(ConfigItem dataStoreConfig : configStore.getConfigItemsByAttributes(filters)){
            dataStoreClients.put(dataStoreConfig.getConfigId(), new DataStoreClient(dataStoreConfig ));
        }
    }

    private void connectRpcTransport() throws IllegalAccessException, ClassNotFoundException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        rpcServer = TransportFactory.newRpcServer(endpointConfig, new RpcRequestHandler(MetaStore.class, callableMetaStore));
        new Thread(rpcServer).start();
    }

    public DataStoreClient getDataStoreClient(String dataStoreClientId) {
        return dataStoreClients.get(dataStoreClientId);
    }

    public void startAggregatorAPI(){
        aggregatorAPI = new AggregatorAPI(callableMetaStore);
    }

    public ConfigItem getEndpointConfig() {
        return endpointConfig;
    }

    @Override
    public void run() {
        try {

            // Connect to the backend metastore
            logger.trace("Connecting to the Aggregator meta store");
            createCallableAggregator();

            // Create and connect the datastore clients
            logger.trace("Connecting to the datastore clients");
            connectDataStoreClients();

            // Start up the RPC transport
            logger.trace("Connecting to the RPC Transport");
            connectRpcTransport();

            logger.trace("Starting the Aggregator API");
            startAggregatorAPI();

        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String createResource(String performanceTier, long dataSize, String dataType, Map<String, Object> metaMap) throws Exception {
        return null;
    }

    @Override
    public String createResource(ResourceRequest resourceRequest) throws Exception {
        return this.createResource(
                resourceRequest.getPerformanceTier(),
                resourceRequest.getDataSize(),
                resourceRequest.getDataType(),
                resourceRequest.getMetaMap());
    }

    @Override
    public boolean updateResource(stitch.resource.Resource resource) throws Exception {
        return false;
    }

    @Override
    public stitch.resource.Resource getResource(String resourceId) throws Exception {
        return null;
    }

    @Override
    public boolean deleteResource(String resourceId) throws Exception {
        return false;
    }

    @Override
    public List<stitch.resource.Resource> listResources() {
        return null;
    }

    @Override
    public List<stitch.resource.Resource> findResources(String filter) {
        return null;
    }

    @Override
    public byte[] readData(String resourceId) {
        return new byte[0];
    }
}
