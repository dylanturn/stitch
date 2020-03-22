package stitch.datastore;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

public class DataStoreInfo implements Serializable {

    private static final long serialVersionUID = 6543L;

    @SerializedName("id")
    private String id;
    @SerializedName("start_time")
    private long startTime;
    @SerializedName("performance_tier")
    private String performanceTier;
    @SerializedName("instance_class")
    private String instanceClass;
    @SerializedName("used_quota")
    private long usedQuota;
    @SerializedName("hard_quota")
    private long hardQuota;
    @SerializedName("resource_count")
    private long resourceCount;
    @SerializedName("last_seen")
    private long lastSeen;
    @SerializedName("latency")
    private long latency;

    public DataStoreInfo(){}

    public DataStoreInfo(String id){
        this.id = id;
    }

    public String getId(){ return id; }

    public long getStartTime(){ return startTime; }

    public String getPerformanceTier() {
        return performanceTier;
    }

    public String getInstanceClass() {
        return instanceClass;
    }

    public long getUsedQuota() {
        return usedQuota;
    }

    public long getHardQuota() {
        return hardQuota;
    }

    public long getAvailableQuota(){
        return getHardQuota() - getUsedQuota();
    }

    public long getResourceCount() {
        return resourceCount;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public long getLatency() { return latency; }

    public DataStoreInfo setId(String id){
        this.id = id;
        return this;
    }

    public DataStoreInfo setStartTime(long startTime){
        this.startTime = startTime;
        return this;
    }

    public DataStoreInfo setPerformanceTier(String performanceTier){
        this.performanceTier = performanceTier;
        return this;
    }

    public DataStoreInfo setInstanceClass(String instanceClass){
        this.instanceClass = instanceClass;
        return this;
    }

    public DataStoreInfo setUsedQuota(long usedQuota){
        this.usedQuota = usedQuota;
        return this;
    }

    public DataStoreInfo setHardQuota(long hardQuota){
        this.hardQuota = hardQuota;
        return this;
    }

    public DataStoreInfo setResourceCount(long resourceCount){
        this.resourceCount = resourceCount;
        return this;
    }

    public DataStoreInfo setLastSeen(long lastSeen){
        this.lastSeen = lastSeen;
        return this;
    }

    public DataStoreInfo setLatency(long latency){
        this.latency = latency;
        return this;
    }

    public static String toJson(DataStoreInfo dataStoreInfo){
        Gson gson = new Gson();
        return gson.toJson(dataStoreInfo);
    }

    public static DataStoreInfo fromJson(String datastoreInfoJson){
        Gson gson = new Gson();
        return gson.fromJson(datastoreInfoJson, DataStoreInfo.class);
    }

}
