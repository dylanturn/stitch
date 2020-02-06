package stitch.datastore;

import stitch.resource.ResourceStatus;
import stitch.util.EndpointStatus;

import java.util.List;

public class DataStoreStatus extends EndpointStatus {

    private List<ResourceStatus> resourceStatusList;
    private long logicalSizeMB;
    private long totalSizeMB;

    public DataStoreStatus(DataStoreServer server) {
        super(server.getId(), server.getStartTime());
    }

    public List<ResourceStatus> getResourceStatusList(){
        return resourceStatusList;
    }

    public long getLogicalSizeMB() {
        return logicalSizeMB;
    }

    public long getTotalSizeMB() {
        return totalSizeMB;
    }

    public DataStoreStatus setResourceStatusList(List<ResourceStatus> resourceStatusList){
        this.resourceStatusList = resourceStatusList;
        return this;
    }

    public DataStoreStatus addResourceStatus(ResourceStatus resourceStatus) {
        this.resourceStatusList.add(resourceStatus);
        return this;
    }

    public DataStoreStatus setLogicalSizeMB(long logicalSizeMB){
        this.logicalSizeMB = logicalSizeMB;
        return this;
    }

    public DataStoreStatus setTotalSizeMB(long totalSizeMB){
        this.totalSizeMB = totalSizeMB;
        return this;
    }
}
