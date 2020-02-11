package stitch.aggregator.metastore;

import stitch.datastore.DataStoreStatus;
import stitch.datastore.ReplicaStatus;
import stitch.resource.Resource;
import stitch.resource.ResourceCallable;
import stitch.resource.ResourceStatus;
import stitch.util.EndpointStatus;

import java.io.IOException;
import java.util.ArrayList;

public interface MetaStoreCallable extends ResourceCallable {
    String getDataStoreById(String resourceId);
    ArrayList<EndpointStatus> listDataStores();

    void reportDataStoreStatus(DataStoreStatus dataStoreStatus) throws IOException, InterruptedException;
    void registerResourceReplica(String datastoreId, ResourceStatus resourceStatus);
    void unregisterResourceReplica(String datastoerId, String resourceId);
    void updateResourceReplica(String datastoreId, ResourceStatus resourceStatus, ReplicaStatus replicaStatus);
}
