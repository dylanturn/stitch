package stitch.aggregator;

import org.apache.log4j.Logger;
import stitch.aggregator.metastore.MetaStoreCallable;
import stitch.datastore.DataStoreClient;
import stitch.resource.Resource;
import stitch.transport.TransportCallableServer;
import stitch.rpc.RpcRequestHandler;
import stitch.transport.TransportFactory;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.item.ConfigItemType;
import stitch.util.configuration.store.ConfigStore;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class AggregatorServer implements Runnable {

    static final Logger logger = Logger.getLogger(AggregatorServer.class);

    private ConfigStore configStore;
    protected ConfigItem endpointConfig;
    protected TransportCallableServer rpcServer;
    protected HashMap<String, DataStoreClient> dataStoreClients = new HashMap<>();
    private MetaStoreCallable callableMetaStore;

    public AggregatorServer(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        configStore = ConfigStore.loadConfigStore();
        this.endpointConfig = endpointConfig;
    }

    private void createCallableAggregator() throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        Class<? extends MetaStoreCallable> metaStoreCallableClass = endpointConfig.getConfigClass("class");
        Constructor<?> metaStoreCallableClassConstructor = metaStoreCallableClass.getConstructor(AggregatorServer.class);
        this.callableMetaStore = (MetaStoreCallable) metaStoreCallableClassConstructor.newInstance(this);
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
        rpcServer = TransportFactory.newRpcServer(endpointConfig, new RpcRequestHandler(MetaStoreCallable.class, callableMetaStore));
        new Thread(rpcServer).start();
    }

    public DataStoreClient getDataStoreClient(String dataStoreClientId) {
        return dataStoreClients.get(dataStoreClientId);
    }

    public ConfigItem getEndpointConfig() {
        return endpointConfig;
    }

    /*private void registerResources(){
        for(Map.Entry<String, DataStoreClient> dataStoreClient : dataStoreClients.entrySet()){
            ArrayList<Resource> resourceArray = dataStoreClient.getValue().listResources();
            for(Resource resource : resourceArray) {
                callableMetaStore.registerResource(dataStoreClient.getKey(), resource);
            }
        }
    }*/

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

            // Request a list of resources from all the DataStores and put them in the cache.
            // logger.trace("Registering resources");
            // this.registerResources();

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

}
