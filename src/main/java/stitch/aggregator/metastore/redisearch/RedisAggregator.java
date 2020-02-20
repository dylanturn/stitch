package stitch.aggregator.metastore.redisearch;

import io.redisearch.*;
import org.apache.log4j.Logger;

import stitch.aggregator.AggregatorServer;
import stitch.aggregator.metastore.MetaStoreCallable;
import stitch.datastore.DataStoreInfo;
import stitch.datastore.DataStoreStatus;
import stitch.datastore.ReplicaStatus;
import stitch.resource.Resource;
import stitch.resource.ResourceStatus;
import stitch.util.configuration.item.ConfigItem;

import java.io.IOException;
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

    @Override
    public String createResource(Resource resource) {
        // TODO: Make this method accept a performance tier parameter.
        DataStoreInfo selectedStore = metaCacheManager.selectEligibleDataStore("general", resource.getDataSize());
        try {
            aggregatorServer.getDataStoreClient(selectedStore.getId()).createResource(resource);
        } catch (Exception error) {
            logger.error("Failed to create the resource!", error);
        }
        return null;
    }

    @Override
    public Resource getResource(String resourceId) {
        try {
            return aggregatorServer.getDataStoreClient(getResourceStoreById(resourceId)).getResource(resourceId);
        } catch (Exception error){
            metaCacheManager.getResourceClient().deleteDocument(resourceId);
        }
        return null;
    }

    // TODO: Implement this!
    @Override
    public boolean updateResource(Resource resource) {
        try {
            if (aggregatorServer.getDataStoreClient(getResourceStoreById(resource.getID())).updateResource(resource)) {
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
            if (aggregatorServer.getDataStoreClient(getResourceStoreById(resourceId)).deleteResource(resourceId))
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
                resourceList.add(aggregatorServer.getDataStoreClient(document.getString("datastore_id"))
                        .getResource(document.getString("uuid")));
            }
            return resourceList;
        } catch (Exception error){

        }
        return null;
    }

    @Override
    public ArrayList<Resource> listResources() {
        Query query = new Query("*");
        ArrayList<Resource> resourceList = new ArrayList<>();
        for(Document document : metaCacheManager.getResourceClient().search(query).docs){
            HashMap<String, Object> resourceMeta = new HashMap<>();
            resourceMeta.put("resource_id", document.getId());
            resourceMeta.put("master_store", metaCacheManager.lookupMasterDataStoreId(document.getString("resource_id")));
            resourceMeta.put("active_stores", metaCacheManager.lookupDataStoreIds(document.getId(), ReplicaStatus.ACTIVE));
            resourceMeta.put("inactive_stores", metaCacheManager.lookupDataStoreIds(document.getId(), ReplicaStatus.INACTIVE));
            resourceMeta.put("data_type", document.getString("data_type"));
            resourceMeta.put("data_size", Integer.parseInt(document.getString("data_size")));
            resourceMeta.put("created", Long.parseLong(document.getString("created")));
            resourceList.add(new Resource(document.getId(), resourceMeta, null));
        }
        logger.info(String.format("%d Resource found!", resourceList.size()));
        return resourceList;
    }

    public String getResourceStoreById(String resourceId) {
        return metaCacheManager.lookupMasterDataStoreId(resourceId);
    }

    @Override
    public DataStoreInfo getDatastore(String datastoreId) {
        return metaCacheManager.getDataStoreInfo(datastoreId);
    }

    @Override
    public ArrayList<DataStoreInfo> listDataStores() {
        ArrayList<DataStoreInfo> datastoreList = new ArrayList<>();
        for(Document document : metaCacheManager.getDatastoreClient().search(new Query("*")).docs){
            DataStoreInfo dataStoreInfo = new DataStoreInfo(document.getString("datastore_id"))
                    .setPerformanceTier(document.getString("performance_tier"))
                    .setInstanceClass(document.getString("instance_class"))
                    .setUsedQuota(Long.valueOf(document.getString("used_quota")))
                    .setHardQuota(Long.valueOf(document.getString("hard_quota")))
                    .setResourceCount(Long.valueOf(document.getString("resource_count")))
                    .setLastSeen(Long.valueOf(document.getString("last_seen")));
            datastoreList.add(dataStoreInfo);
        }
        return datastoreList;
    }

    @Override
    public DataStoreInfo[] findDataStores(String query) {
        return metaCacheManager.searchDatastores(query);
    }

    @Override
    public void reportDataStoreStatus(DataStoreStatus dataStoreStatus) throws IOException, InterruptedException {
        metaCacheManager.cacheDataStoreMeta(dataStoreStatus);
        for(ResourceStatus resourceStatus : dataStoreStatus.getResourceStatuses()){
            metaCacheManager.cacheResourceMeta(dataStoreStatus.getEndpointId(), resourceStatus);
        }
    }
}
