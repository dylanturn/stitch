package stitch.aggregator.metastore.redisearch;

import io.redisearch.*;
import org.apache.log4j.Logger;

import stitch.aggregator.AggregatorServer;
import stitch.aggregator.metastore.DataStoreNotFoundException;
import stitch.aggregator.metastore.MetaStore;
import stitch.datastore.DataStoreInfo;
import stitch.datastore.DataStoreStatus;
import stitch.datastore.ReplicaRole;
import stitch.resource.Resource;
import stitch.resource.ResourceRequest;
import stitch.util.configuration.item.ConfigItem;

import java.io.IOException;
import java.util.*;

public class RedisAggregator implements MetaStore {

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
    public Resource getResource(String resourceId) {
        try {
            return aggregatorServer.getDataStoreClient(getResourceStoreById(resourceId)).getResource(resourceId);
        } catch (Exception error){
            metaCacheManager.getResourceClient().deleteDocument(resourceId);
        }
        return null;
    }

    @Override
    public String createResource(String performanceTier, long dataSize, String dataType, Map<String, Object> metaMap) throws Exception {
        DataStoreInfo selectedStore = metaCacheManager.selectEligibleDataStore(performanceTier, dataSize);
        return aggregatorServer.getDataStoreClient(selectedStore.getId()).createResource(performanceTier, dataSize, dataType, metaMap);
    }

    @Override
    public String createResource(ResourceRequest resourceRequest) throws Exception {
        DataStoreInfo selectedStore = metaCacheManager.selectEligibleDataStore(resourceRequest.getPerformanceTier(), resourceRequest.getDataSize());
        return aggregatorServer.getDataStoreClient(selectedStore.getId()).createResource(resourceRequest);
    }

    // TODO: Implement this!
    @Override
    public boolean updateResource(Resource resource) {
        try {
            if (aggregatorServer.getDataStoreClient(getResourceStoreById(resource.getId())).updateResource(resource)) {
                return true;
            }
        }catch (Exception error){
            logger.error("Encountered failure while updating resource. Resource will be removed from cache.", error);
            metaCacheManager.getResourceClient().deleteDocument(resource.getId());
        }
        return false;
    }

    @Override
    public boolean updateResource(String resourceId, ResourceRequest resourceRequest) throws Exception {
        try {
            if (aggregatorServer.getDataStoreClient(getResourceStoreById(resourceId)).updateResource(resourceId, resourceRequest)) {
                return true;
            }
        }catch (Exception error){
            logger.error("Encountered failure while updating resource. Resource will be removed from cache.", error);
            metaCacheManager.getResourceClient().deleteDocument(resourceId);
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
                        .getResource(document.getString("resource_id")));
            }
            return resourceList;
        } catch (Exception error){

        }
        return null;
    }

    @Override
    public byte[] readData(String resourceId) throws DataStoreNotFoundException {
        return aggregatorServer.getDataStoreClient(getResourceStoreById(resourceId)).readData(resourceId);
    }

    @Override
    public byte[] readData(String resourceId, long offset, long length) {
        return new byte[0];
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes) throws DataStoreNotFoundException {
        return aggregatorServer.getDataStoreClient(getResourceStoreById(resourceId)).writeData(resourceId, dataBytes);
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes, long offset) {
        return 0;
    }

    @Override
    public ArrayList<Resource> listResources() {
        Query query = new Query("*");
        ArrayList<Resource> resourceList = new ArrayList<>();

        for(Document document : metaCacheManager.getResourceClient().search(query).docs){
            logger.info("RESOURCE FOUND");
            HashMap<String, Object> resourceMeta = new HashMap<>();
            resourceMeta.put("master_store", metaCacheManager.lookupMasterDataStoreId(document.getString("resource_id")));
            resourceMeta.put("active_stores", metaCacheManager.lookupDataStoreIds(document.getId(), ReplicaRole.ACTIVE));
            resourceMeta.put("inactive_stores", metaCacheManager.lookupDataStoreIds(document.getId(), ReplicaRole.INACTIVE));

            resourceList.add(new Resource(
                    document.getString("resource_id"),
                    Long.parseLong(document.getString("created")),
                    Long.parseLong(document.getString("mtime")),
                    Long.parseLong(document.getString("epoch")),
                    Long.parseLong(document.getString("data_size")),
                    document.getString("data_type"),
                    document.getString("performance_tier"),
                    resourceMeta
            ));

        }
        logger.info(String.format("%d Resource found!", resourceList.size()));
        return resourceList;
    }

    public String getResourceStoreById(String resourceId) throws DataStoreNotFoundException {
        String storeId = metaCacheManager.lookupMasterDataStoreId(resourceId);
        if(storeId == null)
            throw new DataStoreNotFoundException(storeId);
        return storeId;
    }

    @Override
    public DataStoreInfo getDatastore(String datastoreId) {
        return metaCacheManager.getDataStoreInfo(datastoreId);
    }

    @Override
    public DataStoreInfo[] listDataStores() {
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
        return datastoreList.toArray(new DataStoreInfo[0]);
    }

    @Override
    public DataStoreInfo[] findDataStores(String query) {
        return metaCacheManager.searchDatastores(query);
    }

    @Override
    public void reportDataStoreStatus(DataStoreStatus dataStoreStatus) throws IOException, InterruptedException {
        metaCacheManager.cacheDataStoreMeta(dataStoreStatus);
        for(Resource resourceStatus : dataStoreStatus.getResources()){
            metaCacheManager.cacheResourceMeta(dataStoreStatus.getId(), resourceStatus);
        }
    }
}
