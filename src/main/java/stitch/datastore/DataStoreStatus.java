package stitch.datastore;

import stitch.util.EndpointStatus;

public class DataStoreStatus extends EndpointStatus {

    private long totalStorageSpace;
    private long usedStorageSpace;
    private long resourceCount;

    public DataStoreStatus(String id, long startTime) {
        super(id, startTime);
    }
}
