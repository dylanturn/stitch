package stitch.aggregator;

import stitch.aggregator.metastore.MetaStoreCallable;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class AggregatorFactory {

    public static AggregatorServer create(ConfigItem endpointConfig) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class<? extends MetaStoreCallable> aggregatorClientClass = endpointConfig.getConfigClass("class");
        Constructor<?> aggregatorClientClassConstructor = aggregatorClientClass.getConstructor(ConfigItem.class);
        return (AggregatorServer)aggregatorClientClassConstructor.newInstance(endpointConfig);
    }

    public static AggregatorServer create(String endpointId) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        ConfigItem endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        return AggregatorFactory.create(endpointConfig);
    }
}
