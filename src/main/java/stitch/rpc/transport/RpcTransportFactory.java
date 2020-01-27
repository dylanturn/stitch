package stitch.rpc.transport;

import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class RpcTransportFactory {

    public static RpcCallableClient newRpcClient(String endpointId) throws IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        ConfigItem endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        ConfigItem transportConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointConfig.getConfigString("transport"));
        Class<? extends RpcCallableClient> rpcClientClass =  transportConfig.getConfigClass("client_class");
        Constructor<?> rpcClientClassConstructor = rpcClientClass.getConstructor(ConfigItem.class);
        return (RpcCallableClient)rpcClientClassConstructor.newInstance(endpointConfig);
    }

    public static RpcCallableServer newRpcServer(String endpointId, RpcRequestHandler rpcRequestHandler) throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException {
        ConfigItem endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        ConfigItem transportConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointConfig.getConfigString("transport"));
        Class<? extends RpcCallableServer> rpcServerClass = transportConfig.getConfigClass("server_class");
        Constructor<?> rpcServerClassConstructor = rpcServerClass.getConstructor(ConfigItem.class, RpcRequestHandler.class);
        return (RpcCallableServer)rpcServerClassConstructor.newInstance(endpointConfig, rpcRequestHandler);
    }

}