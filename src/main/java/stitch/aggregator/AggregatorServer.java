package stitch.aggregator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import stitch.aggregator.metastore.MetaCacheManager;
import stitch.aggregator.metastore.MetaStore;
import stitch.datastore.DataStoreClient;
import stitch.transport.TransportCallableServer;
import stitch.rpc.RpcRequestHandler;
import stitch.transport.TransportFactory;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.item.ConfigItemType;
import stitch.util.configuration.store.ConfigStore;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class AggregatorServer implements Runnable {

    private static final Logger logger = LogManager.getLogger(AggregatorServer.class);

    private ConfigStore configStore;
    protected ConfigItem config;
    protected TransportCallableServer rpcServer;
    protected HashMap<String, DataStoreClient> dataStoreClients = new HashMap<>();
    private MetaStore callableMetaStore;
    private AggregatorAPI aggregatorAPI;

    public AggregatorServer(ConfigItem config) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        configStore = ConfigStore.loadConfigStore();
        this.config = config;
    }

    private void createCallableAggregator() throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        callableMetaStore = new MetaCacheManager(this);
    }

    private void connectDataStoreClients() throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        // Create a Map that holds all the filters we'll use to get the aggregators datastores.
        Map<String, String> filters = new HashMap<>();
        filters.put("type", ConfigItemType.DATASTORE.toString());
        filters.put("aggregator", config.getConfigId());
        for(ConfigItem dataStoreConfig : configStore.getConfigItemsByAttributes(filters)){
            dataStoreClients.put(dataStoreConfig.getConfigId(), new DataStoreClient(dataStoreConfig ));
        }
    }

    private void connectRpcTransport() throws IllegalAccessException, ClassNotFoundException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        rpcServer = TransportFactory.newRpcServer(config, new RpcRequestHandler(MetaStore.class, callableMetaStore));
        Thread rpcServerThread = new Thread(rpcServer);
        rpcServerThread.setName(config.getConfigName());
        rpcServerThread.start();
    }

    public void startAggregatorAPI(){
        aggregatorAPI = new AggregatorAPI(callableMetaStore, config.getConfigInt("api_port"));
    }


    public DataStoreClient getDataStoreClient(String dataStoreClientId) {
        return dataStoreClients.get(dataStoreClientId);
    }
    public DataStoreClient[] getDataStoreClients() {
        return  dataStoreClients.values().toArray(new DataStoreClient[0]);
    }
    public ConfigItem getConfig() {
        return config;
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
            logger.error("Access not allowed!", e);
        } catch (ClassNotFoundException e) {
            logger.error("Failed to find class!", e);
        } catch (InstantiationException e) {
            logger.error("Failed to instantiate object!", e);
        } catch (InvocationTargetException e) {
            logger.error("Failed to invoke!", e);
        } catch (NoSuchMethodException e) {
            logger.error("No such method found!", e);
        }
    }
}