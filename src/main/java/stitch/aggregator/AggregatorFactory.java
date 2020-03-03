package stitch.aggregator;

import stitch.aggregator.metastore.MetaStore;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class AggregatorFactory {

    public static AggregatorServer create(AggregatorServer aggregatorServer) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class<? extends MetaStore> aggregatorClientClass = aggregatorServer.getEndpointConfig().getConfigClass("class");
        Constructor<?> aggregatorClientClassConstructor = aggregatorClientClass.getConstructor(AggregatorServer.class);
        return (AggregatorServer)aggregatorClientClassConstructor.newInstance(aggregatorServer);
    }
}
