package stitch.resource;

import stitch.util.Serializer;

import java.io.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Resource implements Serializable {

    private static final long serialVersionUID = 1234L;

    private String id;
    private long epoch;
    private long mtime;
    private long logicalSizeMB;
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

    public String getID(){ return this.id; }

    public long getEpoch() {
        return epoch;
    }

    public long getMtime() {
        return mtime;
    }

    public long getLogicalSizeMB() {
        return logicalSizeMB;
    }

    public byte[] getData(){ return this.data; }

    public void setData(byte[] dataBytes) {

        this.mtime = Instant.now().toEpochMilli();
        this.epoch++;
        this.logicalSizeMB = this.data.length;

        this.metaMap.replace("created", Instant.now().toEpochMilli());
        this.metaMap.replace("data_size", dataBytes.length);
        this.data = dataBytes;
    }

    public void putMeta(String metaKey, Object metaData){
        this.mtime = Instant.now().toEpochMilli();
        this.epoch++;
        this.logicalSizeMB = this.data.length;

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