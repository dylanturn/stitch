package stitch.resource;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ResourceRequest implements Serializable {

    private static final long serialVersionUID = 8746L;

    @SerializedName("epoch")
    long epoch = 0;
    @SerializedName("performance_tier")
    String performanceTier = null;
    @SerializedName("data_size")
    long dataSize = -1;
    @SerializedName("data_type")
    String dataType = null;
    @SerializedName("meta_map")
    Map<String, Object> metaMap = new HashMap<>();

    public ResourceRequest() {}

    public long getEpoch(){ return epoch; }
    public String getPerformanceTier(){ return performanceTier; }
    public long getDataSize(){ return dataSize; }
    public  String getDataType(){ return dataType; }
    public Map<String, Object> getMetaMap(){
        if(metaMap == null)
            return new HashMap<>();
        else
            return metaMap;
    }

    public ResourceRequest setEpoch(long epoch){
        this.epoch = epoch;
        return this;
    }

    public ResourceRequest setPerformanceTier(String performanceTier){
        this.performanceTier = performanceTier;
        return this;
    }

    public ResourceRequest setDataSize(long dataSize){
        this.dataSize = dataSize;
        return this;
    }

    public ResourceRequest setDataType(String dataType){
        this.dataType = dataType;
        return this;
    }

    public ResourceRequest setMetaMap(Map<String, Object> metaMap){
        this.metaMap = metaMap;
        return this;
    }

    public ResourceRequest putMetaItem(String metaKey, Object metaObject){
        this.metaMap.put(metaKey, metaObject);
        return this;
    }

    public static String toJson(ResourceRequest resourceRequest){
        Gson gson = new Gson();
        return gson.toJson(resourceRequest);
    }

    public static ResourceRequest fromJson(String resourceRequestJson){
        Gson gson = new Gson();
        return gson.fromJson(resourceRequestJson, ResourceRequest.class);
    }

}
