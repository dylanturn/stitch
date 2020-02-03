package stitch.aggregator.metastore;

import stitch.datastore.DataStoreReport;
import stitch.resource.Resource;
import stitch.resource.ResourceCallable;

import java.util.ArrayList;

public interface MetaStoreCallable extends ResourceCallable {
    String getDataStoreById(String resourceId);
    ArrayList<DataStoreReport> listDataStores();
    void registerResource(String datastoreId, Resource resource);
}
