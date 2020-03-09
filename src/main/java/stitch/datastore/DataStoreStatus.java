package stitch.datastore;

import stitch.datastore.resource.Resource;
import stitch.util.HealthAlarm;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DataStoreStatus extends DataStoreInfo {

    private long reportTime;
    private List<HealthAlarm> alarmList = new ArrayList<>();
    private List<Resource> resourceList = new ArrayList<>();

    public DataStoreStatus(DataStoreServer server) {
        reportTime = Instant.now().toEpochMilli();
        setId(server.getId());
        setStartTime(server.getStartTime());
        setPerformanceTier(server.getResourceManager().getPerformanceTier());
        setUsedQuota(server.getResourceManager().getUsedQuota());
        setHardQuota(server.getResourceManager().getHardQuota());
        alarmList.addAll(server.listAlarms());
        resourceList.addAll(server.getResourceManager().listResources());
    }

    public long getReportTime() { return reportTime; }
    public HealthAlarm[] getAlarms(){ return alarmList.toArray(new HealthAlarm[0]); }
    public Resource[] getResources(){ return resourceList.toArray(new Resource[0]); }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), getPerformanceTier(), getUsedQuota(), getHardQuota(), getResourceCount());
    }
}