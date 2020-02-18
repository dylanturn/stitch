package stitch.datastore;

import com.google.gson.Gson;

import java.io.Serializable;

public class DataStoreInfo implements Serializable {

    private static final long serialVersionUID = 6543L;

    private String datastoreId;
    private String performanceTier;
    private String instanceClass;
    private long usedQuota;
    private long hardQuota;
    private long resourceCount;
    private long lastSeen;

    public DataStoreInfo(){}

    public DataStoreInfo(String id){
        this.datastoreId = id;
    }

    public String getId(){ return datastoreId; }

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

    public long getResourceCount() {
        return resourceCount;
    }

    public long getLastSeen() {
        return lastSeen;
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

    public static String toJson(DataStoreInfo dataStoreInfo){
        Gson gson = new Gson();
        return gson.toJson(dataStoreInfo);
    }

    public static DataStoreInfo fromJson(String datastoreInfoJson){
        Gson gson = new Gson();
        return gson.fromJson(datastoreInfoJson, DataStoreInfo.class);
    }

}
