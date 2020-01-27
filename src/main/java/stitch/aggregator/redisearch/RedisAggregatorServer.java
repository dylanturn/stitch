package stitch.aggregator.redisearch;

import io.redisearch.client.Client;
import io.redisearch.Document;
import io.redisearch.SearchResult;
import io.redisearch.Query;
import io.redisearch.Schema;
import org.apache.log4j.Logger;

import stitch.aggregator.Aggregator;
import stitch.aggregator.AggregatorServer;
import stitch.resource.Resource;
import stitch.rpc.metrics.RpcEndpointReport;
import stitch.util.configuration.item.ConfigItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RedisAggregatorServer extends AggregatorServer {

    static final Logger logger = Logger.getLogger(RedisAggregatorServer.class);

    private Client redisearchClient;

    public RedisAggregatorServer(ConfigItem aggregatorConfig) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        super(aggregatorConfig);

        String redisearchHost = aggregatorConfig.getConfigString("host");
        int redisearchPort = aggregatorConfig.getConfigInt("port");
        String redisearchIndexPrefix = aggregatorConfig.getConfigString(("indexPrefix"));
        int redisearchPoolSize = aggregatorConfig.getConfigInt(("poolSize"));
        String redisearchPassword = aggregatorConfig.getConfigSecret("password", "", "");
        String redisearchIndex = String.format("%s_%s", redisearchIndexPrefix, aggregatorConfig.getConfigId());

        logger.info("Start a new Aggregator instance...");
        logger.info("Class: " + aggregatorConfig.getConfigString("class"));
        logger.info("Type: " + aggregatorConfig.getConfigString("type"));
        logger.info("UUID:  " + aggregatorConfig.getConfigId());
        logger.info("Host: " + redisearchHost);
        logger.info("Port: " + redisearchPort);
        logger.info("Index: " + redisearchIndex);
        logger.info("PoolSize: " + redisearchPoolSize);

        this.redisearchClient = new Client(redisearchIndex, redisearchHost, redisearchPort);

        // This is the wrong way to do this, but I can't find a better way.
        // TODO: Find a better way to figure out if an index needs to be created.
        try {
            redisearchClient.getInfo();
        } catch (Exception error){
            Schema schema = new Schema()
                    .addTextField("datastoreId", 4.0)
                    .addTextField("uuid", 5.0)
                    .addNumericField("created")
                    .addTextField("data_type", 1.0)
                    .addNumericField("data_size");
            redisearchClient.createIndex(schema, Client.IndexOptions.defaultOptions());
        }

    }

    @Override
    public String createResource(Resource resource) {
        return null;
    }

    @Override
    public Resource getResource(String resourceId) {
        try {
            return dataStoreClients.get(getResourceProviderId(resourceId)).getResource(resourceId);
        } catch (Exception error){
            redisearchClient.deleteDocument(resourceId);
        }
        return null;
    }

    @Override
    public boolean updateResource(Resource resource) {
        try {
            if (dataStoreClients.get(getResourceProviderId(resource.getUUID())).updateResource(resource)) {
                registerResource(getResourceProviderId(resource.getUUID()), resource);
                return true;
            }
        }catch (Exception error){
            logger.error("Encountered failure while updating resource. Resource will be removed from cache.", error);
            redisearchClient.deleteDocument(resource.getUUID());
        }
        return false;
    }

    @Override
    public boolean deleteResource(String resourceId) {
        try {
            if (dataStoreClients.get(getResourceProviderId(resourceId)).deleteResource(resourceId))
                return redisearchClient.deleteDocument(resourceId);
        } catch (Exception error) {
            logger.error("Encountered failure while deleting resource. Resource will be removed from cache.", error);
            redisearchClient.deleteDocument(resourceId);
        }
        return false;
    }

    @Override
    public ArrayList<Resource> findResources(String filter) {
        try {
            ArrayList<Resource> resourceList = new ArrayList<>();
            for (Document document : redisearchClient.search(new Query(filter)).docs) {
                resourceList.add(dataStoreClients.get(document.getString("datastoreId")).getResource(document.getString("uuid")));
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
        for(Document document : redisearchClient.search(query).docs){
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

    // TODO: This should work the way it implies.
    @Override
    public Iterable<Resource> listResources(boolean includeData) {
        return listResources();
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
        redisearchClient.addDocument(resource.getUUID(), 0.5, resourceDocument, false, true, null);
    }

    protected String getResourceProviderId(String resourceId) {
        String queryString = String.format("@uuid:%s", resourceId);
        Query query = new Query(queryString);
        SearchResult searchResult = redisearchClient.search(query);
        return searchResult.docs.get(0).getString("datastoreId");
    }
}