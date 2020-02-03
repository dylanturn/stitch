package stitch.datastore;

import stitch.transport.metrics.RpcEndpointReport;

import java.io.IOException;

public interface DataStoreCallable {
    RpcEndpointReport getEndpointReport() throws IOException, InterruptedException, ClassNotFoundException;
    long getResourceCount();
    long getUsedStorage();
}
