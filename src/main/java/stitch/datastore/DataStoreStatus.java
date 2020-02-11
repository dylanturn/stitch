package stitch.datastore;

import stitch.resource.Resource;
import stitch.resource.ResourceStatus;
import stitch.util.EndpointStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DataStoreStatus extends EndpointStatus {

    private String performanceTier;
    private long usedQuota = -1;
    private long hardQuota = -1;
    private List<ResourceStatus> resourceStatusList = new ArrayList<>();

    public DataStoreStatus(DataStoreServer server) {
        super(server.getId(), server.getStartTime());
        performanceTier = server.getPerformanceTier();
        usedQuota = server.getUsedQuota();
        hardQuota = server.getHardQuota();
        for(Resource resource : server.listResources()){
            resourceStatusList.add(resource.getStatus());
        }
    }

    public String getPerformanceTier() { return performanceTier; }
    public long getUsedQuota() {
        return usedQuota;
    }
    public long getHardQuota() {
        return hardQuota;
    }
    public long getResourceCount() { return resourceStatusList.size(); }
    public ResourceStatus[] getResourceStatuses(){
        return resourceStatusList.toArray(new ResourceStatus[0]);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), getPerformanceTier(), getUsedQuota(), getHardQuota(), getResourceCount());
    }
}