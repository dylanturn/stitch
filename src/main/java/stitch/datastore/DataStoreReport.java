package stitch.datastore;

import stitch.transport.metrics.RpcEndpointReport;
import stitch.transport.metrics.RpcEndpointReporter;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.item.ConfigItemType;

public class DataStoreReport extends RpcEndpointReport {

    private ConfigItem configItem;
    private long totalStorageSpace;
    private long usedStorageSpace;
    private long resourceCount;

    public DataStoreReport(ConfigItem configItem, RpcEndpointReporter rpcEndpointReporter) {
        super(rpcEndpointReporter);
        this.configItem = configItem;
        totalStorageSpace = (long)rpcEndpointReporter.getExtra().get("totalStorageSpace");
        usedStorageSpace = (long)rpcEndpointReporter.getExtra().get("usedStorageSpace");
        resourceCount = (long)rpcEndpointReporter.getExtra().get("resourceCount");
    }

    public ConfigItemType getDataStoreType() {
        return configItem.getConfigType();
    }

    public Class getDataStoreClass() throws ClassNotFoundException {
        return configItem.getConfigClass("class");
    }

    public long getTotalStorageSpace(){
        return totalStorageSpace;
    }

    public long getUsedStorageSpace() {
        return usedStorageSpace;
    }

    public long getAvailableStorageSpace() {
        return totalStorageSpace - usedStorageSpace;
    }

    public long getResourceCount() {
        return resourceCount;
    }
}
