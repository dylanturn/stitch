package stitch.aggregator;

import stitch.datastore.DataStoreClient;
import stitch.resource.Resource;
import stitch.rpc.metrics.RpcEndpointReport;
import stitch.rpc.transport.RpcCallableServer;
import stitch.rpc.transport.RpcRequestHandler;
import stitch.rpc.transport.RpcTransportFactory;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.item.ConfigItemType;
import stitch.util.configuration.store.ConfigStore;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class AggregatorServer implements Aggregator, Runnable {

    private ConfigStore configStore;
    protected ConfigItem endpointConfig;
    protected RpcCallableServer rpcServer;

    protected HashMap<String, DataStoreClient> dataStoreClients = new HashMap<>();

    public static AggregatorServer createAggregator(String endpointId) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        ConfigItem endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        Class<? extends AggregatorServer> aggregatorClientClass = endpointConfig.getConfigClass("class");
        Constructor<?> aggregatorClientClassConstructor = aggregatorClientClass.getConstructor(ConfigItem.class);
        return (AggregatorServer)aggregatorClientClassConstructor.newInstance(endpointConfig);
    }

    public AggregatorServer(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        configStore = ConfigStore.loadConfigStore();
        this.endpointConfig = endpointConfig;
    }

    private void connectRpcTransport() throws IllegalAccessException, ClassNotFoundException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        rpcServer = RpcTransportFactory.newRpcServer(endpointConfig.getConfigId(), new RpcRequestHandler(this));
        new Thread(rpcServer).start();
    }


    private void connectDataStoreClients() throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        // Create a Map that holds all the filters we'll use to get the aggregators datastores.
        Map<String, String> filters = new HashMap<>();
        filters.put("type", ConfigItemType.DATASTORE.toString());
        filters.put("aggregator", endpointConfig.getConfigId());

        for(ConfigItem dataStoreConfig : configStore.getConfigItemsByAttributes(filters)){
            dataStoreClients.put(dataStoreConfig.getConfigId(), new DataStoreClient(dataStoreConfig.getConfigId()));
        }
    }

    private void registerResources(){
        for(Map.Entry<String, DataStoreClient> dataStoreClient : dataStoreClients.entrySet()){
            ArrayList<Resource> resourceArray = dataStoreClient.getValue().listResources();
            for(Resource resource : resourceArray) {
                this.registerResource(dataStoreClient.getKey(), resource);
            }
        }
    }

    @Override
    public ArrayList<RpcEndpointReport> listDataStores() {
        ArrayList<RpcEndpointReport> dataStoreReports = new ArrayList<>();
        for(String dataStoreId : dataStoreClients.keySet()){
            try {
                dataStoreReports.add(dataStoreClients.get(dataStoreId).getEndpointReport());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return dataStoreReports;
    }

    @Override
    public RpcEndpointReport getEndpointReport(){
        return rpcServer.generateEndpointReport();
    }

    @Override
    public void run() {
        try {

            // Start up the RPC transport
            connectRpcTransport();

            // Create and connect the datastore clients
            connectDataStoreClients();

            // Request a list of resources from all the DataStores and put them in the cache.
            this.registerResources();

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
