package stitch.datastore;


import stitch.aggregator.AggregatorServer;
import stitch.rpc.metrics.RpcEndpointReport;
import stitch.rpc.transport.RpcCallableServer;
import stitch.rpc.transport.RpcRequestHandler;
import stitch.rpc.transport.RpcTransportFactory;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public abstract class DataStoreServer implements DataStore, Runnable {

    private ConfigStore configStore;
    protected ConfigItem endpointConfig;
    protected RpcCallableServer rpcServer;

    public static DataStoreServer createDataStore(String endpointId) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        ConfigItem endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        Class<? extends AggregatorServer> dataStoreClientClass = endpointConfig.getConfigClass("class");
        Constructor<?> dataStoreClientClassConstructor = dataStoreClientClass.getConstructor(ConfigItem.class);
        return (DataStoreServer)dataStoreClientClassConstructor.newInstance(endpointConfig);
    }

    public DataStoreServer(ConfigItem configItem) throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException {
        configStore = ConfigStore.loadConfigStore();
        endpointConfig = configItem;
        rpcServer = RpcTransportFactory.newRpcServer(endpointConfig.getConfigId(), new RpcRequestHandler(this));
        new Thread(rpcServer).run();
    }

    @Override
    public void run() {
        // Do whatever needs to be done to connect to the DataStore backend.
        this.connect();
    }

    @Override
    public RpcEndpointReport getEndpointReport(){
        return rpcServer.generateEndpointReport();
    }

    public abstract void connect();
}
