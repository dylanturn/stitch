package stitch.datastore;

import stitch.resource.Resource;
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
        setPerformanceTier(server.getPerformanceTier());
        setUsedQuota(server.getUsedQuota());
        setHardQuota(server.hardQuota);
        alarmList.addAll(server.listAlarms());
        resourceList.addAll(server.listResources());
    }

    public long getReportTime() { return reportTime; }
    public HealthAlarm[] getAlarms(){ return alarmList.toArray(new HealthAlarm[0]); }
    public Resource[] getResources(){ return resourceList.toArray(new Resource[0]); }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), getPerformanceTier(), getUsedQuota(), getHardQuota(), getResourceCount());
    }
}