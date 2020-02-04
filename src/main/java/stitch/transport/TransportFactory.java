package stitch.transport;

import stitch.rpc.RpcRequestHandler;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class TransportFactory {

    public static TransportCallableClient newRpcClient(String endpointId) throws IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        ConfigItem endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        ConfigItem transportConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointConfig.getConfigString("transport"));
        Class<? extends TransportCallableClient> rpcClientClass =  transportConfig.getConfigClass("client_class");
        Constructor<?> rpcClientClassConstructor = rpcClientClass.getConstructor(ConfigItem.class);
        return (TransportCallableClient)rpcClientClassConstructor.newInstance(endpointConfig);
    }

    public static TransportCallableServer newRpcServer(String endpointId, TransportHandler transportHandler) throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException {
        ConfigItem endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        ConfigItem transportConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointConfig.getConfigString("transport"));
        Class<? extends TransportCallableServer> rpcServerClass = transportConfig.getConfigClass("server_class");
        Constructor<?> rpcServerClassConstructor = rpcServerClass.getConstructor(ConfigItem.class, TransportHandler.class);
        return (TransportCallableServer)rpcServerClassConstructor.newInstance(endpointConfig, transportHandler);
    }

}