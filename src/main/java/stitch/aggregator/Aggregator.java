package stitch.aggregator;

import stitch.amqp.HealthReport;
import stitch.util.Resource;

import java.util.ArrayList;

public interface Aggregator {

    ArrayList<HealthReport> listDataStores();

    String createResource(Resource resource);
    void updateResource(Resource resource);
    Resource getResource(String resourceId);
    void deleteResource(String resourceId);
    ArrayList<Resource> findResources(String filter);
    ArrayList<Resource> listResources();
    void registerResource(String datastoreId, Resource resource);

}
