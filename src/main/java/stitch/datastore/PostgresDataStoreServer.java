package stitch.datastore;

import org.bson.Document;
import stitch.util.HealthReport;
import stitch.util.Resource;

public class PostgresDataStoreServer extends DataStoreServer {

    public PostgresDataStoreServer(Document providerArgs) throws Exception {
        super(providerArgs);
    }

    @Override
    public void connect() {

    }

    @Override
    public HealthReport reportHealth() {
        return null;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public String createResource(Resource resource) throws Exception {
        return null;
    }

    @Override
    public boolean updateResource(Resource resource) throws Exception {
        return false;
    }

    @Override
    public Resource getResource(String resourceId) throws Exception {
        return null;
    }

    @Override
    public boolean deleteResource(String resourceId) throws Exception {
        return false;
    }

    @Override
    public Iterable<Resource> listResources() {
        return null;
    }

    @Override
    public Iterable<Resource> listResources(boolean includeData) {
        return null;
    }
}