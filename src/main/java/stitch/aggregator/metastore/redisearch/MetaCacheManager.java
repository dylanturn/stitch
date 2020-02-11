package stitch.aggregator.metastore.redisearch;

import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;
import stitch.datastore.DataStoreStatus;
import stitch.datastore.ReplicaStatus;
import stitch.resource.ResourceStatus;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MetaCacheManager {

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
                    .addTextField("resourceId", 5.0)
                    .addTextField("datastoreId", 4.0)
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
        datastoreFields.put("last_hash", dataStoreStatus.hashCode());
        datastoreFields.put("last_seen", Instant.now().toEpochMilli());

        if(datastoreCache != null && datastoreCache.hasProperty("last_hash")) {
            // If the hash hasn't changed then we just update the two last fields and call it a day.
            if (Integer.valueOf(datastoreCache.getString("last_hash")) == dataStoreStatus.hashCode()) {
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

        if(datastoreCache == null && datastoreCache.hasProperty("last_hash")){
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
            if (Integer.valueOf(resourceCache.getString("last_hash")) == resourceStatus.hashCode()) {
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

        if(resourceCache == null && resourceCache.hasProperty("last_hash")){
            // Create the resource cache from scratch
            this.resourceSchemaClient.addDocument(resourceStatus.getId(), resourceFields);
            updateReplicaMap(datastoreId, resourceStatus.getId(), ReplicaStatus.MASTER);
        } else {
            // Pass the resource update with all of the fields we're replacing.
            this.resourceSchemaClient.replaceDocument(resourceStatus.getId(), 0.5f, resourceFields);
            updateReplicaMap(datastoreId, resourceStatus.getId());
        }
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


        if(replicaStatus == ReplicaStatus.MASTER){
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

    protected String lookupDataStoreId(String resourceId){
        return lookupDataStoreId(resourceId, ReplicaStatus.MASTER);
    }

    protected String lookupDataStoreId(String resourceId, ReplicaStatus replicaStatus){
        String queryString = String.format("@resource_id:%s @replica_status:%s", resourceId, replicaStatus.toString());
        Query query = new Query(queryString);
        SearchResult searchResult = mapSchemaClient.search(query);
        return searchResult.docs.get(0).getString("datastore_id");
    }
}
