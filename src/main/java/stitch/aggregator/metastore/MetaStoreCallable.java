package stitch.aggregator.metastore;

import stitch.datastore.DataStoreInfo;
import stitch.datastore.DataStoreStatus;
import stitch.datastore.ReplicaStatus;
import stitch.resource.Resource;
import stitch.resource.ResourceCallable;
import stitch.resource.ResourceStatus;
import stitch.util.EndpointStatus;

import java.io.IOException;
import java.util.ArrayList;

public interface MetaStoreCallable extends ResourceCallable {

    DataStoreInfo getDatastore(String datastoreId);
    Iterable<DataStoreInfo> listDataStores();
    DataStoreInfo[] findDataStores(String query);

    void reportDataStoreStatus(DataStoreStatus dataStoreStatus) throws IOException, InterruptedException;
}