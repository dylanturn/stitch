package stitch.rpc.transport;

import stitch.rpc.metrics.RpcEndpointReport;
import stitch.rpc.metrics.RpcEndpointReporter;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

public abstract class RpcCallableAbstract {

    protected ConfigItem endpointConfig;
    protected ConfigItem transportConfig;
    protected RpcEndpointReporter rpcEndpointReporter;

    public RpcCallableAbstract(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        ConfigStore configStore = ConfigStore.loadConfigStore();
        this.endpointConfig = endpointConfig;
        this.transportConfig = configStore.getConfigItemById(endpointConfig.getConfigString("transport"));
        rpcEndpointReporter = new RpcEndpointReporter(endpointConfig);
    }

    public String getRpcAddress(){
        return String.format("%s_%s", endpointConfig.getConfigType().toString(), endpointConfig.getConfigId());
    }

    public abstract boolean isReady();
    public abstract boolean isConnected();

    public RpcEndpointReport generateEndpointReport(){
        return rpcEndpointReporter.generateReport();
    }
}
