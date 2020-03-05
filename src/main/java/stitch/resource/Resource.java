package stitch.resource;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import stitch.util.Serializer;

import java.io.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Resource implements Serializable {

    private static final long serialVersionUID = 1234L;

    @SerializedName("resource_id")
    private String id = "unknown";
    @SerializedName("created")
    private long created = 0;
    @SerializedName("mtime")
    private long mtime = 0;
    @SerializedName("epoch")
    private long epoch = 0;
    @SerializedName("data_size")
    private long dataSize = 0;
    @SerializedName("data_type")
    private String dataType = "unknown";
    @SerializedName("performance_tier")
    private String performanceTier = "unknown";
    @SerializedName("meta_map")
    private Map<String, Object> metaMap = new HashMap<>();

    public Resource(){}
    public Resource(String id, long created, long mtime, long epoch, long dataSize, String dataType, String performanceTier, Map<String, Object> metaMap) {
        this.id = id;
        this.created = created;
        this.mtime = mtime;
        this.epoch = epoch;
        this.dataSize = dataSize;
        this.dataType = dataType;
        this.performanceTier = performanceTier;
        this.metaMap = metaMap;
    }

    public long getEpoch(){
        return epoch;
    }

    // ------ Meta Map STUFF ------
    public Object getMeta(String metaKey){
        return this.metaMap.get(metaKey);
    }
    public String getMetaString(String metaKey){
        return String.valueOf(getMeta(metaKey));
    }
    public int getMetaInt(String metaKey){
        return Integer.parseInt(getMetaString(metaKey));
    }
    public long getMetaLong(String metaKey){
        return Long.parseLong(getMetaString(metaKey));
    }
    public Map<String, Object> getMetaMap(){
        return this.metaMap;
    }
    public Resource putMeta(String metaKey, Object metaData){
        incrementEpoch();
        this.metaMap.put(metaKey, metaData);
        return this;
    }
    public Resource setMeta(Map<String, Object> metaMap){
        incrementEpoch();
        this.metaMap = metaMap;
        return this;
    }

    // ------ Resource ID STUFF ------
    public String getId(){ return this.id; }
    public Resource setId(String id){
        incrementEpoch();
        this.id = id;
        return this;
    }

    // ------ Created Date STUFF ------
    public long getCreated(){ return created; }
    public Resource setCreated(long created){
        incrementEpoch();
        this.created = created;
        return this;
    }

    // ------ Modified Time STUFF ------
    public long getMtime() { return mtime; }
    public Resource setMtime(long mtime) {
        incrementEpoch();
        this.mtime = mtime;
        return this;
    }

    // ------ Data Size STUFF ------
    public long getDataSize() { return dataSize; }
    public Resource setDataSize(long dataSize) {
        incrementEpoch();
        this.dataSize = dataSize;
        return this;
    }

    // ------ Data Type STUFF ------
    public String getDataType() { return dataType; }
    public Resource setDataType(String dataType){
        incrementEpoch();
        this.dataType = dataType;
        return this;
    }

    // ------ Performance Tier STUFF ------
    public String getPerformanceTier(){ return performanceTier; }
    public Resource setPerformanceTier(String performanceTier){
        incrementEpoch();
        this.performanceTier = performanceTier;
        return this;
    }

    public Resource updateResource(ResourceRequest resourceRequest){

        boolean resourceUpdated = false;

        if(resourceRequest.getPerformanceTier() != null) {
            this.performanceTier = resourceRequest.performanceTier;
            resourceUpdated = true;
        }
        if(resourceRequest.getDataSize() > -1) {
            this.dataSize = resourceRequest.dataSize;
            resourceUpdated = true;
        }
        if(resourceRequest.getDataType() != null) {
            this.dataType = resourceRequest.dataType;
            resourceUpdated = true;
        }
        if(resourceRequest.getMetaMap().size() > 0) {
            this.metaMap = resourceRequest.metaMap;
            resourceUpdated = true;
        }

        if(resourceUpdated){
            incrementEpoch();
        }

        return this;
    }

    private void incrementEpoch(){
        this.epoch++;
        this.mtime = Instant.now().toEpochMilli();
    }

    public static Resource newResource(long dataSize, String dataType, String performanceTier, Map<String, Object> metaMap){
        return new Resource()
                .setId(UUID.randomUUID().toString().replace("-", ""))
                .setCreated(Instant.now().toEpochMilli())
                .setDataSize(dataSize)
                .setDataType(dataType)
                .setPerformanceTier(performanceTier)
                .setMeta(metaMap);
    }

    public static String toJson(Resource resource){
        Gson gson = new Gson();
        return gson.toJson(resource);
    }

    public static Resource fromJson(String resourceJson){
        Gson gson = new Gson();
        return gson.fromJson(resourceJson, Resource.class);
    }
}