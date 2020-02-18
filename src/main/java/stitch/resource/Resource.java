package stitch.resource;

import com.google.gson.Gson;
import stitch.datastore.DataStoreInfo;
import stitch.util.Serializer;

import java.io.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Resource implements Serializable {

    private static final long serialVersionUID = 1234L;

    private String id;
    private Map<String, Object> metaMap = new HashMap<>();
    private byte[] data;

    public Resource(String dataType, byte[] dataBytes){
        this.id = UUID.randomUUID().toString().replace("-", "");
        putMeta("created", Instant.now().toEpochMilli());
        putMeta("data_type", dataType);
        putMeta("data_size", dataBytes.length);
        this.data = dataBytes;
    }

    public Resource(String id, HashMap<String, Object> metaMap, byte[] dataBytes) {
        this.id = id;
        this.metaMap = metaMap;
        this.data = dataBytes;
    }

    private void incrementEpoch(){
        long currentEpoch = getEpoch();
        currentEpoch++;
        this.metaMap.replace("epoch", currentEpoch);
        this.metaMap.replace("mtime", Instant.now().toEpochMilli());
    }

    public void putMeta(String metaKey, Object metaData){
        incrementEpoch();
        this.metaMap.put(metaKey, metaData);
    }

    public void setData(byte[] dataBytes) {
        incrementEpoch();
        this.metaMap.replace("created", Instant.now().toEpochMilli());
        this.metaMap.replace("data_size", dataBytes.length);
        this.data = dataBytes;
    }

    public String getID(){ return this.id; }
    public long getCreated(){ return this.getMetaLong("created"); }
    public long getEpoch() { return getMetaLong("epoch"); }
    public long getMtime() { return getMetaLong("mtime"); }
    public ResourceStatus getStatus(){
        return new ResourceStatus(id)
                .setCreated(getCreated())
                .setDataType(getDataType())
                .setDataSize(getDataSize())
                .setEpoch(getEpoch())
                .setMtime(getMtime());
    }

    public String getDataType() { return this.getMetaString("data_type"); }
    public long getDataSize() { return this.getMetaLong("data_size"); }
    public long getLogicalSizeMB() { return getDataSize(); }
    public byte[] getData(){ return this.data; }
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

    public static Resource fromByteArray(byte[] resourceBytes) throws IOException, ClassNotFoundException {
        return (Resource) Serializer.bytesToObject(resourceBytes);
    }

    public static byte[] toByteArray(Resource resource) throws IOException {
        return Serializer.objectToBytes(resource);
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