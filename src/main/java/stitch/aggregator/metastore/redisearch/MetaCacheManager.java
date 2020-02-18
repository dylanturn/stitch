package stitch.aggregator.metastore.redisearch;

import io.redisearch.*;
import io.redisearch.client.Client;
import org.apache.log4j.Logger;
import stitch.datastore.DataStoreInfo;
import stitch.datastore.DataStoreStatus;
import stitch.datastore.ReplicaStatus;
import stitch.resource.ResourceStatus;

import java.time.Instant;
import java.util.*;

public class MetaCacheManager {

    static final Logger logger = Logger.getLogger(MetaCacheManager.class);

    private Client resourceSchemaClient;
    private Client datastoreSchemaClient;
    private Client mapSchemaClient;

    public MetaCacheManager(String redisearchHost, int redisearchPort){
        this.resourceSchemaClient = initializeResourceSchema(redisearchHost, redisearchPort);
        this.datastoreSchemaClient = connectDataStoreSchema(redisearchHost, redisearchPort);
        this.mapSchemaClient = connectMapSchema(redisearchHost, redisearchPort);
    }

    private Client initializeResourceSchema(String host, int port){
        Client client = new Client("resource_meta", host, port);
        try {
            client.getInfo();
        } catch (Exception error) {
            Schema resourceSchema = new Schema()
                    .addTextField("resource_id", 5.0)
                    .addTextField("datastore_id", 4.0)
                    .addNumericField("created")
                    .addTextField("data_type", 1.0)
                    .addNumericField("data_size")
                    .addNumericField("epoch")
                    .addNumericField("mtime")
                    .addNumericField("last_hash")
                    .addNumericField("last_seen");
            client.createIndex(resourceSchema, Client.IndexOptions.defaultOptions());
        }
        return client;
    }

    private Client connectDataStoreSchema(String host, int port){
        Client client = new Client("datastore_meta", host, port);
        try {
            client.getInfo();
        } catch (Exception error) {
            Schema datastoreSchema = new Schema()
                    .addTextField("datastore_id", 5.0)
                    .addTextField("performance_tier", 3.0)
                    .addTextField("instance_class", 1.0)
                    .addNumericField("used_quota")
                    .addNumericField("hard_quota")
                    .addNumericField("resource_count")
                    .addNumericField("last_hash")
                    .addNumericField("last_seen");
            client.createIndex(datastoreSchema, Client.IndexOptions.defaultOptions());
        }
        return client;
    }

    private Client connectMapSchema(String host, int port){
        Client client = new Client("meta_map", host, port);
        try {
            client.getInfo();
        } catch (Exception error) {
            Schema mapSchema = new Schema()
                    .addTextField("resource_id", 5.0)
                    .addTextField("datastore_id", 4.0)
                    .addTextField("replica_status", 3.0)
                    .addNumericField("last_hash")
                    .addNumericField("last_seen");
            client.createIndex(mapSchema, Client.IndexOptions.defaultOptions());
        }
        return client;
    }

    private String calculateDocumentHashId(String dataStoreId, String resourceId){
        return String.valueOf(Objects.hash(dataStoreId, resourceId));
    }

    protected void cacheDataStoreMeta(DataStoreStatus dataStoreStatus){

        // Try get the cache item from redisearch. NULL will be returned if it doesn't exist.
        Document datastoreCache = this.datastoreSchemaClient.getDocument(dataStoreStatus.getEndpointId());

        // Create a hashmap with the last hash and update vaues. These get committed either way.
        Map<String, Object> datastoreFields = new HashMap<>();
        datastoreFields.put("datastore_id", dataStoreStatus.getEndpointId());
        datastoreFields.put("last_hash", dataStoreStatus.hashCode());
        datastoreFields.put("last_seen", Instant.now().toEpochMilli());

        // If the document exists and has a last_hash property, then we'll just update it.
        if(datastoreCache != null && datastoreCache.hasProperty("last_hash")) {
            // If the hash hasn't changed then we just update the two last fields and call it a day.
            if (Integer.valueOf(datastoreCache.getString("last_hash")).equals(dataStoreStatus.hashCode())) {
                this.datastoreSchemaClient.updateDocument(dataStoreStatus.getEndpointId(), 0.5f, datastoreFields);
                return;
            }
        }

        // If we get here then we've got to update the rest of the fields.
        datastoreFields.put("performance_tier", dataStoreStatus.getPerformanceTier());
        datastoreFields.put("instance_class", dataStoreStatus.getClass().toString());
        datastoreFields.put("hard_quota", dataStoreStatus.getHardQuota());
        datastoreFields.put("used_quota", dataStoreStatus.getUsedQuota());
        datastoreFields.put("resource_count", dataStoreStatus.getResourceCount());

        // If the document doesn't exist we'll create it. Otherwise we'll replace it.
        if(datastoreCache == null){
            // Create the cache document from scratch
            this.datastoreSchemaClient.addDocument(dataStoreStatus.getEndpointId(), datastoreFields);
        } else {
            // Pass the update with all of the fields we're replacing.
            this.datastoreSchemaClient.replaceDocument(dataStoreStatus.getEndpointId(), 0.5f, datastoreFields);
        }
    }

    protected void cacheResourceMeta(String datastoreId, ResourceStatus resourceStatus){

        Document resourceCache = this.datastoreSchemaClient.getDocument(resourceStatus.getId());

        Map<String, Object> resourceFields = new HashMap<>();
        resourceFields.put("last_hash", resourceStatus.hashCode());
        resourceFields.put("last_seen", Instant.now().toEpochMilli());

        if(resourceCache != null && resourceCache.hasProperty("last_hash")) {
            if (Integer.valueOf(resourceCache.getString("last_hash")).equals(resourceStatus.hashCode())) {
                this.resourceSchemaClient.updateDocument(resourceStatus.getId(), 0.5f, resourceFields);
                updateReplicaMap(datastoreId, resourceStatus.getId());
                return;
            }
        }

        resourceFields.put("resource_id", resourceStatus.getId());
        resourceFields.put("created", resourceStatus.getCreated());
        resourceFields.put("data_type", resourceStatus.getDataType());
        resourceFields.put("data_size", resourceStatus.getDataSize());
        resourceFields.put("epoch", resourceStatus.getEpoch());
        resourceFields.put("mtime", resourceStatus.getMtime());

        if(resourceCache == null){
            // Create the resource cache from scratch
            this.resourceSchemaClient.addDocument(resourceStatus.getId(), resourceFields);
            updateReplicaMap(datastoreId, resourceStatus.getId(), ReplicaStatus.MASTER);
        } else {
            // Pass the resource update with all of the fields we're replacing.
            this.resourceSchemaClient.replaceDocument(resourceStatus.getId(), 0.5f, resourceFields);
            updateReplicaMap(datastoreId, resourceStatus.getId());
        }
    }

    protected Client getDatastoreClient(){
        return datastoreSchemaClient;
    }
    protected Client getResourceClient(){
        return resourceSchemaClient;
    }

    protected void updateReplicaMap(String datastoreId, String resourceId){
        String documentHash = calculateDocumentHashId(datastoreId, resourceId);
        Document replicaMap = this.mapSchemaClient.getDocument(documentHash);

        if(replicaMap == null){
            updateReplicaMap(datastoreId, resourceId, ReplicaStatus.MASTER);
            return;
        }

        ReplicaStatus replicaStatus;
        if(replicaMap.getString("replica_status") == null){
            replicaStatus = ReplicaStatus.MASTER;
        } else {
            replicaStatus = ReplicaStatus.valueOf(replicaMap.getString("replica_status"));
        }


        if(replicaStatus.toString().equals(ReplicaStatus.MASTER.toString())){
            updateReplicaMap(datastoreId, resourceId, replicaStatus);
        } else {
            updateReplicaMap(datastoreId, resourceId, ReplicaStatus.ACTIVE);
        }
    }

    protected void updateReplicaMap(String datastoreId, String resourceId, ReplicaStatus replicaStatus){
        String documentHash = calculateDocumentHashId(datastoreId, resourceId);
        Document replicaMap = this.mapSchemaClient.getDocument(documentHash);

        Map<String, Object> mapFields = new HashMap<>();
        mapFields.put("resource_id", resourceId);
        mapFields.put("datastore_id", datastoreId);
        mapFields.put("replica_status", replicaStatus.toString());
        mapFields.put("last_hash", documentHash);
        mapFields.put("last_seen", Instant.now().toEpochMilli());

        if(replicaMap == null){
            this.mapSchemaClient.addDocument(documentHash, mapFields);
        } else {
            this.mapSchemaClient.replaceDocument(documentHash, 0.5f, mapFields);
        }
    }

    // TODO: Fix. The issue was that we're trying to look up the replica by resource id when we're passing the
    protected String lookupMasterDataStoreId(String resourceId){
        String[] matchedStores = lookupDataStoreIds(resourceId, ReplicaStatus.MASTER);
        if(matchedStores.length == 0)
            logger.warn("No masters found!");
        if(matchedStores.length > 1)
            logger.error("Multiple masters found!");
        return matchedStores[0];
    }

    protected DataStoreInfo getDataStoreInfo(String datastoreId){
        SearchResult searchResult = datastoreSchemaClient.search(new Query("*"));
        for(Document document : searchResult.docs){
            if(datastoreId.equals(document.getString("datastore_id"))){
                return new DataStoreInfo(document.getString("datastore_id"))
                        .setPerformanceTier(document.getString("performance_tier"))
                        .setInstanceClass(document.getString("instance_class"))
                        .setUsedQuota(Long.valueOf(document.getString("used_quota")))
                        .setHardQuota(Long.valueOf(document.getString("hard_quota")))
                        .setResourceCount(Long.valueOf(document.getString("resource_count")))
                        .setLastSeen(Long.valueOf(document.getString("last_seen")));
            }
        }
        return null;
    }

    protected String[] lookupDataStoreIds(String resourceId, ReplicaStatus replicaStatus){

        SearchResult searchResult = mapSchemaClient.search(new Query("*"));
        List<String> dataStoreIds = new ArrayList<>();

        for(Document document : searchResult.docs){
            String docResourceId = document.getString("resource_id");
            String docReplicaStatus = document.getString("replica_status");
            if(docResourceId.equals(resourceId))
                if(docReplicaStatus.equals(replicaStatus.toString()))
                    dataStoreIds.add(document.getString("datastore_id"));
        }
        if(dataStoreIds.size() > 0) {
            return dataStoreIds.toArray(new String[0]);
        } else {
            logger.warn("No replicas found!");
            return null;
        }
    }
}
