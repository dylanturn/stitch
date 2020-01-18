package stitch.resource;

import stitch.util.Serializer;

import java.io.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Resource implements Serializable {

    private static final long serialVersionUID = 1234L;

    private String uuid;
    private Map<String, Object> metaMap = new HashMap<>();
    private byte[] data;

    public Resource(String dataType, byte[] dataBytes){
        this.uuid = UUID.randomUUID().toString().replace("-", "");
        this.metaMap.put("created", Instant.now().toEpochMilli());
        this.metaMap.put("data_type", dataType);
        this.metaMap.put("data_size", dataBytes.length);
        this.data = dataBytes;
    }

    public Resource(String uuid, HashMap<String, Object> metaMap, byte[] dataBytes) {
        this.uuid = uuid;
        this.metaMap = metaMap;
        this.data = dataBytes;
    }

    public String getUUID(){ return this.uuid; }

    public byte[] getData(){ return this.data; }
    public void setData(byte[] dataBytes) {
        this.metaMap.replace("created", Instant.now().toEpochMilli());
        this.metaMap.replace("data_size", dataBytes.length);
        this.data = dataBytes;
    }

    public void putMeta(String metaKey, Object metaData){
        this.metaMap.put(metaKey, metaData);
    }
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
}