package stitch.resource;

import java.util.List;

public class ResourceStatus {
    private String id;
    private long epoch;
    private long mtime;
    private long logicalSizeMB;

    private String master;
    private List<String> activeDataStores;
    private List<String> inactiveDataStores;
    private List<String> unusedDataStores;

    public ResourceStatus(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public long getEpoch() {
        return epoch;
    }

    public long getMtime() {
        return mtime;
    }

    public long getLogicalSizeMB() {
        return logicalSizeMB;
    }

    public String getMaster() {
        return master;
    }

    public List<String> getActiveDataStores() {
        return activeDataStores;
    }

    public List<String> getInactiveDataStores() {
        return inactiveDataStores;
    }

    public List<String> getUnusedDataStores() {
        return unusedDataStores;
    }

    public ResourceStatus setEpoch(long epoch){
        this.epoch = epoch;
        return this;
    }

    public ResourceStatus setMtime(long mtime) {
        this.mtime = mtime;
        return this;
    }

    public ResourceStatus setLogicalSizeMB(long logicalSizeMB) {
        this.logicalSizeMB = logicalSizeMB;
        return this;
    }

    public ResourceStatus setMaster(String master) {
        this.master = master;
        return this;
    }

    public ResourceStatus setActiveDataStores(List<String> activeDataStores) {
        this.activeDataStores = activeDataStores;
        return this;
    }

    public ResourceStatus addActiveDataStore(String activeDataStore) {
        this.activeDataStores.add(activeDataStore);
        return this;
    }

    public ResourceStatus setInactiveDataStores(List<String> inactiveDataStores) {
        this.inactiveDataStores = inactiveDataStores;
        return this;
    }

    public ResourceStatus addInactiveDataStores(String inactiveDataStore) {
        this.inactiveDataStores.add(inactiveDataStore);
        return this;
    }

    public ResourceStatus setUnusedDataStores(List<String> unusedDataStores) {
        this.unusedDataStores = unusedDataStores;
        return this;
    }

    public ResourceStatus addUnusedDataStores(String unusedDataStore) {
        this.unusedDataStores.add(unusedDataStore);
        return this;
    }

}
