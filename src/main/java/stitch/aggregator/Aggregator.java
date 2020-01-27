package stitch.aggregator;

import stitch.rpc.metrics.RpcEndpointReport;
import stitch.datastore.DataStore;
import stitch.resource.Resource;

import java.util.ArrayList;

public interface Aggregator extends DataStore {
    ArrayList<RpcEndpointReport> listDataStores();
    void registerResource(String datastoreId, Resource resource);
}
