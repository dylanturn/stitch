package stitch.aggregator.metastore;

import stitch.resource.Resource;
import stitch.resource.ResourceCallable;
import stitch.util.EndpointStatus;

import java.util.ArrayList;

public interface MetaStoreCallable extends ResourceCallable {
    String getDataStoreById(String resourceId);
    ArrayList<EndpointStatus> listDataStores();
    void registerResource(String datastoreId, Resource resource);
}
