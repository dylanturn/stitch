package stitch.aggregator;

import stitch.amqp.HealthReport;
import stitch.resource.Resource;

import java.util.ArrayList;

public interface Aggregator {

    ArrayList<HealthReport> listDataStores();

    String createResource(Resource resource);
    boolean updateResource(Resource resource);
    Resource getResource(String resourceId);
    boolean deleteResource(String resourceId);
    ArrayList<Resource> findResources(String filter);
    ArrayList<Resource> listResources();
    void registerResource(String datastoreId, Resource resource);

}
