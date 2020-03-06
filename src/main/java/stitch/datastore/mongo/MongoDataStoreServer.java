package stitch.datastore.mongo;

import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.result.UpdateResult;
import org.apache.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.Document;
import stitch.datastore.resource.Resource;
import stitch.datastore.resource.ResourceRequest;
import stitch.datastore.resource.ResourceStoreProvider;
import stitch.util.configuration.item.ConfigItem;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MongoDataStoreServer implements ResourceStoreProvider {

    static final Logger logger = Logger.getLogger(MongoDataStoreServer.class);

    private String dsURI;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Document> mongoCollection;
    private ConfigItem providerConfig;

    public MongoDataStoreServer(ConfigItem providerConfig) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        this.providerConfig = providerConfig;
        String dsProtocol = providerConfig.getConfigString("protocol");
        String dsHost = providerConfig.getConfigString("host");
        int dsPort = providerConfig.getConfigInt("port");
        String dsUsername = providerConfig.getConfigString("username");
        String dsPassword = providerConfig.getConfigString("password");
        String dsOptions = providerConfig.getConfigString("options");
        String database = providerConfig.getConfigString("database");
        String collection = providerConfig.getConfigString("collection");

        logger.info("Start a new ResourceStoreProvider instance...");
        logger.info("UUID:  " + providerConfig.getConfigId());
        logger.info("Class: " + providerConfig.getConfigString("class"));
        logger.info("Type: " + providerConfig.getConfigString("type"));
        logger.info("Host: " + providerConfig.getConfigString("host"));
        logger.info("Options: " + providerConfig.getConfigString("options"));
        logger.info("Database: " + providerConfig.getConfigString("database"));
        logger.info("Collection: " + providerConfig.getConfigString("collection"));

        try {
            dsURI = String.format("%s://%s:%s@%s/%s?%s", dsProtocol, dsUsername, dsPassword, dsHost, database, dsOptions);
            MongoClientURI mongoClientURI = new MongoClientURI(dsURI);
            mongoClient = new MongoClient(mongoClientURI);
            mongoDatabase = mongoClient.getDatabase(database);
            this.mongoCollection = mongoDatabase.getCollection(collection);
        } catch(Exception error){
            logger.error("Failed to load the collection!");
            logger.error(error);
        }
    }

    private static Document fromResource(Resource resource){
        Document document = new Document();
        document.put("resource_id", resource.getId());
        document.put("created", resource.getCreated());
        document.put("mtime", resource.getMtime());
        document.put("epoch", resource.getEpoch());
        document.put("data_size", resource.getDataSize());
        document.put("data_type", resource.getDataType());
        document.put("performance_tier", resource.getPerformanceTier());
        Document metaDocument = new Document();
        metaDocument.putAll(resource.getMetaMap());
        document.put("meta_map", metaDocument);
        return document;
    }

    private static Resource toResource(Document document){
        return new Resource(
                document.getString("resource_id"),
                document.getLong("created"),
                document.getLong("mtime"),
                document.getLong("epoch"),
                document.getLong("data_size"),
                document.getString("data_type"),
                document.getString("performance_tier"),
                document.get("meta_map", Document.class)
        );
    }

    @Override
    public String createResource(ResourceRequest resourceRequest) throws Exception {

        logger.trace("Creating resource!");
        MongoDatabase mdb = mongoClient.getDatabase(providerConfig.getConfigString("database"));
        MongoCollection<Document> mcol = mdb.getCollection(providerConfig.getConfigString("collection"));

        Resource resource = new Resource();
        resource.setId(UUID.randomUUID().toString().replace("-", ""))
                .setCreated(Instant.now().toEpochMilli())
                .setDataSize(resourceRequest.getDataSize())
                .setDataType(resourceRequest.getDataType())
                .setPerformanceTier(resourceRequest.getPerformanceTier())
                .setMeta(resourceRequest.getMetaMap());

        logger.trace(String.format("Inserting resource %s into the database...",resource.getId()));
        mcol.insertOne(fromResource(resource));
        return resource.getId();
    }

    @Override
    public boolean updateResource(String resourceId, ResourceRequest resourceRequest) throws Exception {
        logger.trace(String.format("Updating resource: %s", resourceId));
        BasicDBObject query = new BasicDBObject();
        // Get the resource we want to update.
        Resource resource = getResource(resourceId);
        if(resourceRequest.getEpoch() == resource.getEpoch()) {
            resource = resource.updateResource(resourceRequest);
            query.put("resource_id", resourceId);
            return mongoCollection.replaceOne(query, fromResource(resource)).wasAcknowledged();
        } else {
            return false;
        }
    }

    @Override
    public Resource getResource(String resourceId) {
        logger.trace(String.format("Getting resource: %s", resourceId));
        BasicDBObject query = new BasicDBObject();
        query.put("resource_id", resourceId);
        Document resourceObject = mongoCollection.find(query).first();
        return toResource(resourceObject);
    }

    @Override
    public boolean deleteResource(String resourceId) {
        logger.trace(String.format("Deleting resource: %s", resourceId));
        BasicDBObject query = new BasicDBObject();
        query.put("resource_id", resourceId);
        return mongoCollection.deleteOne(query).wasAcknowledged();
    }

    @Override
    public ArrayList<Resource> listResources() {
        logger.trace("Listing Resources");
        ArrayList<Resource> resourceList = new ArrayList<>();
        FindIterable<Document> foundDocuments;
        foundDocuments = mongoCollection.find();
        for(Document document : foundDocuments){
            resourceList.add(toResource(document));
        }
        return resourceList;
    }

    // TODO: Implement some kind of resource search logic.
    @Override
    public ArrayList<Resource> findResources(String filter) {
        return listResources();
    }

    @Override
    public byte[] readData(String resourceId) {
        logger.trace(String.format("Getting resource: %s", resourceId));
        BasicDBObject query = new BasicDBObject();
        query.put("resource_id", resourceId);
        Document resourceObject = mongoCollection.find(query).first();
        return resourceObject.get("data", String.class).getBytes();
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes) throws Exception {
        BasicDBObject query = new BasicDBObject();
        query.put("resource_id", resourceId);
        Resource curResource = getResource(resourceId).incrementEpoch();
        updateResource(resourceId, new ResourceRequest().setEpoch(curResource.getEpoch()).setDataSize(dataBytes.length));
        String updateQuery = String.format("{ $set: { data: \"%s\" } }", new String(dataBytes, "utf-8"));
        UpdateResult updateResult = mongoCollection.updateOne(query, BsonDocument.parse(updateQuery));
        if(updateResult.wasAcknowledged())
            return dataBytes.length;
        return 0;
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes, long offset) {
        return 0;
    }

    public byte[] readData(String resourceId, long offset, long length){
        byte[] dataBytes = readData(resourceId);
        // TODO: THIS STUFF
        return dataBytes;
    }

    @Override
    public boolean isReady() {

        // How can we be ready if we're not even alive?!
        if(!isAlive())
            return false;

        ListCollectionsIterable<Document> mongoCollectionlist = mongoDatabase.listCollections()
                .maxTime(60, TimeUnit.SECONDS);

        // Here we're checking to make sure we return something. If we do then we know the connection is good.
        for(Document document : mongoCollectionlist){
            return true;
        }
        return false;
    }

    @Override
    public boolean isAlive() {
        if(mongoCollection != null)
            return true;
        return false;
    }

}
