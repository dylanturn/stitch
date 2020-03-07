package stitch.aggregator.metastore;

import io.redisearch.*;
import org.apache.log4j.Logger;

import stitch.aggregator.AggregatorServer;
import stitch.datastore.DataStoreInfo;
import stitch.datastore.DataStoreStatus;
import stitch.datastore.resource.ResourceReplicaRole;
import stitch.datastore.resource.Resource;
import stitch.datastore.resource.ResourceRequest;
import stitch.util.configuration.item.ConfigItem;

import java.io.IOException;
import java.util.*;

public class MetaCacheManager implements MetaStore {

    static final Logger logger = Logger.getLogger(MetaCacheManager.class);

    private AggregatorServer aggregatorServer;
    private MetaCacheProvider metaCacheProvider;

    public MetaCacheManager(AggregatorServer aggregatorServer) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
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
        metaCacheProvider = new MetaCacheProvider(redisearchHost, redisearchPort);
    }

    @Override
    public Resource getResource(String resourceId) {
        try {
            return aggregatorServer.getDataStoreClient(getResourceStoreById(resourceId)).getResource(resourceId);
        } catch (Exception error){
            metaCacheProvider.getResourceClient().deleteDocument(resourceId);
        }
        return null;
    }

    @Override
    public String createResource(ResourceRequest resourceRequest) throws Exception {
        DataStoreInfo selectedStore = metaCacheProvider.selectEligibleDataStore(resourceRequest.getPerformanceTier(), resourceRequest.getDataSize());
        return aggregatorServer.getDataStoreClient(selectedStore.getId()).createResource(resourceRequest);
    }

    @Override
    public boolean updateResource(ResourceRequest resourceRequest) throws Exception {
        try {
            if (aggregatorServer.getDataStoreClient(getResourceStoreById(resourceRequest.getId())).updateResource(resourceRequest)) {
                return true;
            }
        }catch (Exception error){
            logger.error("Encountered failure while updating resource. Resource will be removed from cache.", error);
            metaCacheProvider.getResourceClient().deleteDocument(resourceRequest.getId());
        }
        return false;
    }

    // TODO: Implement this!
    @Override
    public boolean deleteResource(String resourceId) {
        try {
            if (aggregatorServer.getDataStoreClient(getResourceStoreById(resourceId)).deleteResource(resourceId))
                return metaCacheProvider.getResourceClient().deleteDocument(resourceId);

        } catch (Exception error) {
            logger.error("Encountered failure while deleting resource. Resource will be removed from cache.", error);
            metaCacheProvider.getResourceClient().deleteDocument(resourceId);
        }
        return false;
    }

    // TODO: Implement this!
    @Override
    public ArrayList<Resource> findResources(String filter) {
        try {
            ArrayList<Resource> resourceList = new ArrayList<>();
            for (Document document : metaCacheProvider.getResourceClient().search(new Query(filter)).docs) {
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

        for(Document document : metaCacheProvider.getResourceClient().search(query).docs){
            logger.info("RESOURCE FOUND");
            HashMap<String, Object> resourceMeta = new HashMap<>();
            resourceMeta.put("master_store", metaCacheProvider.lookupMasterDataStoreId(document.getString("resource_id")));
            resourceMeta.put("active_stores", metaCacheProvider.lookupDataStoreIds(document.getId(), ResourceReplicaRole.ACTIVE));
            resourceMeta.put("inactive_stores", metaCacheProvider.lookupDataStoreIds(document.getId(), ResourceReplicaRole.INACTIVE));

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
        String storeId = metaCacheProvider.lookupMasterDataStoreId(resourceId);
        if(storeId == null)
            throw new DataStoreNotFoundException(storeId);
        return storeId;
    }

    @Override
    public DataStoreInfo getDatastore(String datastoreId) {
        return metaCacheProvider.getDataStoreInfo(datastoreId);
    }

    @Override
    public DataStoreInfo[] listDataStores() {
        ArrayList<DataStoreInfo> datastoreList = new ArrayList<>();
        for(Document document : metaCacheProvider.getDatastoreClient().search(new Query("*")).docs){
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
        return metaCacheProvider.searchDatastores(query);
    }

    @Override
    public void reportDataStoreStatus(DataStoreStatus dataStoreStatus) throws IOException, InterruptedException {
        metaCacheProvider.cacheDataStoreMeta(dataStoreStatus);
        for(Resource resourceStatus : dataStoreStatus.getResources()){
            metaCacheProvider.cacheResourceMeta(dataStoreStatus.getId(), resourceStatus);
        }
    }
}
