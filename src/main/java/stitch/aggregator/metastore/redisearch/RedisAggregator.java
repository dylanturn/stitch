package stitch.aggregator.metastore.redisearch;

import io.redisearch.*;
import io.redisearch.client.Client;
import org.apache.log4j.Logger;

import scala.Int;
import stitch.aggregator.AggregatorServer;
import stitch.aggregator.metastore.MetaStoreCallable;
import stitch.datastore.DataStoreStatus;
import stitch.datastore.ReplicaStatus;
import stitch.resource.Resource;
import stitch.resource.ResourceStatus;
import stitch.util.EndpointStatus;
import stitch.util.configuration.item.ConfigItem;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

public class RedisAggregator implements MetaStoreCallable {

    static final Logger logger = Logger.getLogger(RedisAggregator.class);

    private AggregatorServer aggregatorServer;
    private MetaCacheManager metaCacheManager;

    public RedisAggregator(AggregatorServer aggregatorServer) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        this.aggregatorServer = aggregatorServer;
        ConfigItem aggregatorConfig = aggregatorServer.getEndpointConfig();
        String redisearchHost = aggregatorConfig.getConfigString("host");
        int redisearchPort = aggregatorConfig.getConfigInt("port");
        String redisearchIndexPrefix = aggregatorConfig.getConfigString(("indexPrefix"));
        int redisearchPoolSize = aggregatorConfig.getConfigInt(("poolSize"));
        String redisearchIndex = String.format("%s_%s", redisearchIndexPrefix, aggregatorConfig.getConfigId());
        logger.debug("Start a new AggregatorCallable instance...");
        logger.debug("Class: " + aggregatorConfig.getConfigString("class"));
        logger.debug("Type: " + aggregatorConfig.getConfigString("type"));
        logger.debug("UUID:  " + aggregatorConfig.getConfigId());
        logger.debug("Host: " + redisearchHost);
        logger.debug("Port: " + redisearchPort);
        logger.debug("Index: " + redisearchIndex);
        logger.debug("PoolSize: " + redisearchPoolSize);
        metaCacheManager = new MetaCacheManager(redisearchHost, redisearchPort);
    }

    // TODO: Implement this!
    @Override
    public String createResource(Resource resource) {
        return null;
    }

    // TODO: Implement this!
    @Override
    public Resource getResource(String resourceId) {
        try {
            return aggregatorServer.getDataStoreClient(getDataStoreById(resourceId)).getResource(resourceId);
        } catch (Exception error){
            metaCacheManager.getResourceClient().deleteDocument(resourceId);
        }
        return null;
    }

    // TODO: Implement this!
    @Override
    public boolean updateResource(Resource resource) {
        try {
            if (aggregatorServer.getDataStoreClient(getDataStoreById(resource.getID())).updateResource(resource)) {
                //registerResource(getDataStoreById(resource.getID()), resource);
                return true;
            }
        }catch (Exception error){
            logger.error("Encountered failure while updating resource. Resource will be removed from cache.", error);
            metaCacheManager.getResourceClient().deleteDocument(resource.getID());
        }
        return false;
    }

    // TODO: Implement this!
    @Override
    public boolean deleteResource(String resourceId) {
        try {
            if (aggregatorServer.getDataStoreClient(getDataStoreById(resourceId)).deleteResource(resourceId))
                return metaCacheManager.getResourceClient().deleteDocument(resourceId);

        } catch (Exception error) {
            logger.error("Encountered failure while deleting resource. Resource will be removed from cache.", error);
            metaCacheManager.getResourceClient().deleteDocument(resourceId);
        }
        return false;
    }

    // TODO: Implement this!
    @Override
    public ArrayList<Resource> findResources(String filter) {
        try {
            ArrayList<Resource> resourceList = new ArrayList<>();
            for (Document document : metaCacheManager.getResourceClient().search(new Query(filter)).docs) {
                resourceList.add(aggregatorServer.getDataStoreClient(document.getString("datastoreId")).getResource(document.getString("uuid")));
            }
            return resourceList;
        } catch (Exception error){

        }
        return null;
    }

    @Override
    public ArrayList<Resource> listResources() {
        Query query = new Query("*").limitFields("datastore_id", "resource_id", "data_type", "data_size", "created");
        ArrayList<Resource> resourceList = new ArrayList<>();
        for(Document document : metaCacheManager.getResourceClient().search(query).docs){
            logger.info("Found Resource: " + document.toString());
            String uuid = document.getString("uuid");
            logger.info("Found Resource: " + uuid);
            HashMap<String, Object> resourceMeta = new HashMap<>();
            resourceMeta.put("datastoreId", document.getString("datastore_id"));
            resourceMeta.put("data_type", document.getString("data_type"));
            resourceMeta.put("data_size", Integer.parseInt(document.getString("data_size")));
            resourceMeta.put("created", Long.parseLong(document.getString("created")));
            resourceList.add(new Resource(uuid, resourceMeta, null));
        }
        logger.info(String.format("%d Resource found!", resourceList.size()));
        return resourceList;
    }

    @Override
    public void registerResourceReplica(String datastoreId, ResourceStatus resourceStatus){
        metaCacheManager.cacheResourceMeta(datastoreId, resourceStatus);
    }

    @Override
    public void unregisterResourceReplica(String datastoreId, String resourceId) {
        metaCacheManager.updateReplicaMap(datastoreId, resourceId, ReplicaStatus.INACTIVE);
    }

    @Override
    public void updateResourceReplica(String datastoreId, ResourceStatus resourceStatus, ReplicaStatus replicaStatus){
        metaCacheManager.updateReplicaMap(datastoreId, resourceStatus.getId(), replicaStatus);
    }

    @Override
    public String getDataStoreById(String resourceId) {
        return metaCacheManager.lookupDataStoreId(resourceId);
    }

    @Override
    public ArrayList<EndpointStatus> listDataStores() {
        return null;
    }

    @Override
    public void reportDataStoreStatus(DataStoreStatus dataStoreStatus) throws IOException, InterruptedException {

        System.out.println("----- DataStore Status ------");
        System.out.println("Endpoint Id:      " + dataStoreStatus.getEndpointId());
        System.out.println("Used Quota:       " + dataStoreStatus.getUsedQuota());
        System.out.println("Hard Quota:       " + dataStoreStatus.getHardQuota());
        System.out.println("Resource Count:   " + dataStoreStatus.getResourceCount());
        metaCacheManager.cacheDataStoreMeta(dataStoreStatus);

        System.out.println("----- Resource Status ------");
        for(ResourceStatus resourceStatus : dataStoreStatus.getResourceStatuses()){
            System.out.println("- Resource Id:    " + resourceStatus.getId());
            System.out.println("- Resource Epoch: " + resourceStatus.getEpoch());
            System.out.println("- Resource Mtime: " + resourceStatus.getMtime());
            metaCacheManager.cacheResourceMeta(dataStoreStatus.getEndpointId(), resourceStatus);
        }
    }
}
