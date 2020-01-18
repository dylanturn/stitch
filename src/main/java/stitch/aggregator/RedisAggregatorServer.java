package stitch.aggregator;

import io.redisearch.client.Client;
import io.redisearch.Document;
import io.redisearch.SearchResult;
import io.redisearch.Query;
import io.redisearch.Schema;
import org.apache.log4j.Logger;

import stitch.amqp.HealthReport;
import stitch.resource.Resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RedisAggregatorServer extends AggregatorServer implements Aggregator {

    static final Logger logger = Logger.getLogger(RedisAggregatorServer.class);
    private Client client;

    private String agClass = aggregatorArgs.getString("class");
    private String agType = aggregatorArgs.getString("type");
    private String agUUID = aggregatorArgs.getString("uuid");
    private String agHost = aggregatorArgs.getString("host");
    private int agPort = aggregatorArgs.getInteger("port");
    private String agIndexPrefix = aggregatorArgs.getString("indexPrefix");
    private int agTimeout = aggregatorArgs.getInteger("timeout");
    private int agPoolSize = aggregatorArgs.getInteger("poolSize");
    private String agPassword = aggregatorArgs.getString("password");
    private String agIndex;

    public RedisAggregatorServer(org.bson.Document aggregatorArgs, Iterable<org.bson.Document> providerDocuments) throws Exception {
        super(aggregatorArgs, providerDocuments);
    }

    @Override
    public void connect() {

        agIndex = String.format("%s_%s", agIndexPrefix, agUUID);
        logger.info("Start a new Aggregator instance...");
        logger.info("Class: " + agClass);
        logger.info("Type: " + agType);
        logger.info("UUID:  " + agUUID);
        logger.info("Host: " + agHost);
        logger.info("Port: " + agPort);
        logger.info("Index: " + agIndex);
        logger.info("Timeout: " + agTimeout);
        logger.info("PoolSize: " + agPoolSize);

        this.client = new Client(agIndex,agHost,agPort);

        // This is the wrong way to do this, but I can't find a better way.
        // TODO: Find a better way to figure out if an index needs to be created.
        try {
            client.getInfo();
        } catch (Exception error){
            Schema schema = new Schema()
                    .addTextField("datastoreId", 4.0)
                    .addTextField("uuid", 5.0)
                    .addNumericField("created")
                    .addTextField("data_type", 1.0)
                    .addNumericField("data_size");
            client.createIndex(schema, Client.IndexOptions.defaultOptions());
        }
    }

    @Override
    public void reportHealth(HealthReport healthReport) { }

    @Override
    public String createResource(Resource resource) {
        return null;
    }

    @Override
    public Resource getResource(String resourceId) {
        try {
            return providerClients.get(getResourceProviderId(resourceId)).getResource(resourceId);
        } catch (Exception error){
            client.deleteDocument(resourceId);
        }
        return null;
    }

    @Override
    public boolean updateResource(Resource resource) {
        try {
            if (providerClients.get(getResourceProviderId(resource.getUUID())).updateResource(resource)) {
                registerResource(getResourceProviderId(resource.getUUID()), resource);
                return true;
            }
        }catch (Exception error){
            logger.error("Encountered failure while updating resource. Resource will be removed from cache.", error);
            client.deleteDocument(resource.getUUID());
        }
        return false;
    }

    @Override
    public boolean deleteResource(String resourceId) {
        try {
            if (providerClients.get(getResourceProviderId(resourceId)).deleteResource(resourceId))
                return client.deleteDocument(resourceId);
        } catch (Exception error) {
            logger.error("Encountered failure while deleting resource. Resource will be removed from cache.", error);
            client.deleteDocument(resourceId);
        }
        return false;
    }

    @Override
    public ArrayList<Resource> findResources(String filter) {
        try {
            ArrayList<Resource> resourceList = new ArrayList<>();
            for (Document document : client.search(new Query(filter)).docs) {
                resourceList.add(providerClients.get(document.getString("datastoreId")).getResource(document.getString("uuid")));
            }
            return resourceList;
        } catch (Exception error){

        }
        return null;
    }

    @Override
    public ArrayList<Resource> listResources() {
        Query query = new Query("*").limitFields("datastoreId", "uuid", "data_type", "data_size", "created");
        ArrayList<Resource> resourceList = new ArrayList<>();
        for(Document document : client.search(query).docs){
            logger.info("Found Resource: " + document.toString());
            String uuid = document.getString("uuid");
            logger.info("Found Resource: " + uuid);
            HashMap<String, Object> resourceMeta = new HashMap<>();
            resourceMeta.put("datastoreId", document.getString("datastoreId"));
            resourceMeta.put("data_type", document.getString("data_type"));
            resourceMeta.put("data_size", Integer.parseInt(document.getString("data_size")));
            resourceMeta.put("created", Long.parseLong(document.getString("created")));
            resourceList.add(new Resource(uuid, resourceMeta, null));
        }
        logger.info(String.format("%d Resource found!", resourceList.size()));
        return resourceList;
    }

    @Override
    public void registerResource(String datastoreId, Resource resource) {
        logger.info(String.format("Registering resource %s in datastore %s with redisearch cache", resource.getUUID(), datastoreId));
        Map<String, Object> resourceDocument = new HashMap<>();
        resourceDocument.put("uuid", resource.getUUID());
        resourceDocument.put("datastoreId", datastoreId);
        resourceDocument.putAll(resource.getMetaMap());
        logger.info(String.format("DatastoreId: %s", resourceDocument.get("datastoreId")));
        logger.info(String.format("ResourceId: %s", resourceDocument.get("uuid")));
        client.addDocument(resource.getUUID(), 0.5, resourceDocument, false, true, null);
    }

    protected String getResourceProviderId(String resourceId) {
        String queryString = String.format("@uuid:%s", resourceId);
        Query query = new Query(queryString);
        SearchResult searchResult = client.search(query);
        return searchResult.docs.get(0).getString("datastoreId");
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down aggregator client...");
    }

}
