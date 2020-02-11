package stitch.resource;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class ResourceStatus implements Serializable {

    private static final long serialVersionUID = 5436L;

    private String id;
    private long created;
    private String dataType;
    private long dataSize;
    private long epoch;
    private long mtime;

    public ResourceStatus(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
    public long getCreated() { return created; }
    public String getDataType() { return dataType; }
    public long getDataSize() {
        return dataSize;
    }

    public long getEpoch() { return epoch; }
    public long getMtime() {
        return mtime;
    }

    public ResourceStatus setCreated(long created){
        this.created = created;
        return this;
    }

    public ResourceStatus setDataType(String dataType){
        this.dataType = dataType;
        return this;
    }

    public ResourceStatus setDataSize(long dataSize) {
        this.dataSize = dataSize;
        return this;
    }

    public ResourceStatus setEpoch(long epoch){
        this.epoch = epoch;
        return this;
    }

    public ResourceStatus setMtime(long mtime) {
        this.mtime = mtime;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(created, dataType, dataSize, epoch, mtime);
    }


}
