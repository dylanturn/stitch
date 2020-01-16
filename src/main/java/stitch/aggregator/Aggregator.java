package stitch.aggregator;

import stitch.util.Resource;

import java.util.ArrayList;

public interface Aggregator {

    ArrayList<String> listDataStores();

    String createResource(Resource resource);
    void updateResource(Resource resource);
    Resource getResource(String resourceId);
    void deleteResource(String resourceId);
    ArrayList<Resource> findResources(String filter);
    ArrayList<Resource> listResources();
    void registerResource(String datastoreId, Resource resource);

}
