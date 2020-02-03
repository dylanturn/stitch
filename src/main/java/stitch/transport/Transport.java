package stitch.transport;

import stitch.transport.metrics.RpcEndpointReporter;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

public abstract class Transport  {

    protected ConfigItem endpointConfig;
    protected ConfigItem transportConfig;
    protected RpcEndpointReporter rpcEndpointReporter;

    public Transport(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        ConfigStore configStore = ConfigStore.loadConfigStore();
        this.endpointConfig = endpointConfig;
        this.transportConfig = configStore.getConfigItemById(endpointConfig.getConfigString("transport"));
        rpcEndpointReporter = new RpcEndpointReporter(endpointConfig);
    }

    public String getRpcAddress(){
        return String.format("%s_%s", endpointConfig.getConfigType().toString(), endpointConfig.getConfigId());
    }

    public RpcEndpointReporter getRpcEndpointReporter() {
        return rpcEndpointReporter;
    }

}
