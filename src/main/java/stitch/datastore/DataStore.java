package stitch.datastore;

import stitch.resource.Resource;
import stitch.rpc.metrics.RpcEndpointReport;

import java.io.IOException;
import java.util.ArrayList;

public interface DataStore {
    String createResource(Resource resource) throws Exception;
    boolean updateResource(Resource resource) throws Exception;
    Resource getResource(String resourceId) throws Exception;
    boolean deleteResource(String resourceId) throws Exception;
    Iterable<Resource> listResources();
    Iterable<Resource> listResources(boolean includeData);
    ArrayList<Resource> findResources(String filter);
    RpcEndpointReport getEndpointReport() throws IOException, InterruptedException, ClassNotFoundException;
}
