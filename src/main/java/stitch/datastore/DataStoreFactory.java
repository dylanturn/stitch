package stitch.datastore;

import stitch.aggregator.AggregatorServer;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class DataStoreFactory {

    public static DataStoreServer createDataStore(ConfigItem endpointConfig) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class<? extends AggregatorServer> dataStoreClientClass = endpointConfig.getConfigClass("class");
        Constructor<?> dataStoreClientClassConstructor = dataStoreClientClass.getConstructor(ConfigItem.class);
        return (DataStoreServer) dataStoreClientClassConstructor.newInstance(endpointConfig);
    }

    public static DataStoreServer createDataStore(String endpointId) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        ConfigItem endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        return DataStoreFactory.createDataStore(endpointConfig);
    }

}
