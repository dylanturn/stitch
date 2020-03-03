package stitch.aggregator.metastore;

import stitch.datastore.DataStoreInfo;
import stitch.datastore.DataStoreStatus;
import stitch.resource.ResourceStore;

import java.io.IOException;

public interface MetaStore extends ResourceStore {

    DataStoreInfo getDatastore(String datastoreId);
    DataStoreInfo[] listDataStores();
    DataStoreInfo[] findDataStores(String query);

    void reportDataStoreStatus(DataStoreStatus dataStoreStatus) throws IOException, InterruptedException;
}