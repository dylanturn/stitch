package stitch.aggregator.metastore.redisearch;

import io.redisearch.*;
import io.redisearch.client.Client;
import org.apache.log4j.Logger;

import stitch.aggregator.AggregatorServer;
import stitch.aggregator.metastore.MetaStoreCallable;
import stitch.datastore.DataStoreStatus;
import stitch.resource.Resource;
import stitch.rpc.RpcRequest;
import stitch.util.EndpointStatus;
import stitch.util.configuration.item.ConfigItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisAggregator implements MetaStoreCallable {

    static final Logger logger = Logger.getLogger(RedisAggregator.class);

    private AggregatorServer aggregatorServer;
    private Client redisearchClient;

    public RedisAggregator(AggregatorServer aggregatorServer) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        this.aggregatorServer = aggregatorServer;

        ConfigItem aggregatorConfig = aggregatorServer.getEndpointConfig();
        String redisearchHost = aggregatorConfig.getConfigString("host");
        int redisearchPort = aggregatorConfig.getConfigInt("port");
        String redisearchIndexPrefix = aggregatorConfig.getConfigString(("indexPrefix"));
        int redisearchPoolSize = aggregatorConfig.getConfigInt(("poolSize"));
        String redisearchPassword = aggregatorConfig.getConfigSecret("password", "", "");
        String redisearchIndex = String.format("%s_%s", redisearchIndexPrefix, aggregatorConfig.getConfigId());

        logger.info("Start a new AggregatorCallable instance...");
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
                    .addNumericField("data_size")
                    .addTextField("itemType", 0.5);
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
            return aggregatorServer.getDataStoreClient(getDataStoreById(resourceId)).getResource(resourceId);
        } catch (Exception error){
            redisearchClient.deleteDocument(resourceId);
        }
        return null;
    }

    @Override
    public boolean updateResource(Resource resource) {
        try {
            if (aggregatorServer.getDataStoreClient(getDataStoreById(resource.getID())).updateResource(resource)) {
                registerResource(getDataStoreById(resource.getID()), resource);
                return true;
            }
        }catch (Exception error){
            logger.error("Encountered failure while updating resource. Resource will be removed from cache.", error);
            redisearchClient.deleteDocument(resource.getID());
        }
        return false;
    }

    @Override
    public boolean deleteResource(String resourceId) {
        try {
            if (aggregatorServer.getDataStoreClient(getDataStoreById(resourceId)).deleteResource(resourceId))
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
                resourceList.add(aggregatorServer.getDataStoreClient(document.getString("datastoreId")).getResource(document.getString("uuid")));
            }
            return resourceList;
        } catch (Exception error){

        }
        return null;
    }

    @Override
    public List<Resource> listResources() {
        return listResources(false);
    }

    @Override
    public ArrayList<Resource> listResources(boolean includeData) {

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

            if(includeData) {
                // TODO: This should work the way it implies.
                resourceList.add(new Resource(uuid, resourceMeta, null));
            } else {
                resourceList.add(new Resource(uuid, resourceMeta, null));
            }
        }
        logger.info(String.format("%d Resource found!", resourceList.size()));
        return resourceList;
    }

    @Override
    public void registerResource(String datastoreId, Resource resource) {
        logger.info(String.format("Registering resource %s in datastore %s with redisearch cache", resource.getID(), datastoreId));
        Map<String, Object> resourceDocument = new HashMap<>();
        resourceDocument.put("uuid", resource.getID());
        resourceDocument.put("datastoreId", datastoreId);
        resourceDocument.put("itemType", "resource");
        resourceDocument.putAll(resource.getMetaMap());
        logger.info(String.format("DatastoreId: %s", resourceDocument.get("datastoreId")));
        logger.info(String.format("ResourceId: %s", resourceDocument.get("uuid")));
        redisearchClient.addDocument(resource.getID(), 0.5, resourceDocument, false, true, null);
    }

    @Override
    public String getDataStoreById(String resourceId) {
        String queryString = String.format("@uuid:%s", resourceId);
        Query query = new Query(queryString);
        SearchResult searchResult = redisearchClient.search(query);
        return searchResult.docs.get(0).getString("datastoreId");
    }

    @Override
    public ArrayList<EndpointStatus> listDataStores() {
        return null;
    }

    @Override
    public void reportDataStoreStatus(DataStoreStatus dataStoreStatus) throws IOException, InterruptedException {
        System.out.println(dataStoreStatus.getTotalSizeMB());
    }
}
