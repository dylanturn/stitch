package stitch.resource;

import com.google.gson.Gson;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ResourceRequest implements Serializable {

    private static final long serialVersionUID = 8746L;

    String performanceTier;
    long dataSize;
    String dataType;
    Map<String, Object> metaMap;

    public ResourceRequest() {}

    public String getPerformanceTier(){ return performanceTier; }
    public long getDataSize(){ return dataSize; }
    public  String getDataType(){ return dataType; }
    public Map<String, Object> getMetaMap(){
        if(metaMap == null)
            return new HashMap<>();
        else
            return metaMap;
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
