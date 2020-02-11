package stitch.aggregator;

import stitch.aggregator.metastore.MetaStoreCallable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class AggregatorFactory {

    public static AggregatorServer create(AggregatorServer aggregatorServer) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class<? extends MetaStoreCallable> aggregatorClientClass = aggregatorServer.getEndpointConfig().getConfigClass("class");
        Constructor<?> aggregatorClientClassConstructor = aggregatorClientClass.getConstructor(AggregatorServer.class);
        return (AggregatorServer)aggregatorClientClassConstructor.newInstance(aggregatorServer);
    }
}
