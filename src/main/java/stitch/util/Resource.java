package stitch.util;

import org.apache.log4j.Logger;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Resource implements Serializable {

    private static final long serialVersionUID = 1234L;
    private static final Logger logger = Logger.getLogger(Resource.class);

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
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(resourceBytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        Resource resource = (Resource) objectInputStream.readObject();
        objectInputStream.close();
        return resource;
    }

    public static byte[] toByteArray(Resource resource) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(resource);
        objectOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }
}