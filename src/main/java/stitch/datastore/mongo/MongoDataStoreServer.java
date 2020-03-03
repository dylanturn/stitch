package stitch.datastore.mongo;

import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;
import stitch.datastore.DataStoreServer;
import stitch.resource.Resource;
import stitch.resource.ResourceRequest;
import stitch.util.configuration.item.ConfigItem;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class MongoDataStoreServer extends DataStoreServer {

    static final Logger logger = Logger.getLogger(MongoDataStoreServer.class);

    private String dsURI;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Document> mongoCollection;

    public MongoDataStoreServer(ConfigItem dataStoreConfig) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        super(dataStoreConfig);

        String dsProtocol = endpointConfig.getConfigString("protocol");
        String dsHost = endpointConfig.getConfigString("host");
        int dsPort = endpointConfig.getConfigInt("port");
        String dsUsername = endpointConfig.getConfigString("username");
        String dsPassword = endpointConfig.getConfigString("password");
        String dsOptions = endpointConfig.getConfigString("options");
        String database = endpointConfig.getConfigString("database");
        String collection = endpointConfig.getConfigString("collection");

        logger.info("Start a new DataStore instance...");
        logger.info("UUID:  " + endpointConfig.getConfigId());
        logger.info("Class: " + endpointConfig.getConfigString("class"));
        logger.info("Type: " + endpointConfig.getConfigString("type"));
        logger.info("Host: " + endpointConfig.getConfigString("host"));
        logger.info("Options: " + endpointConfig.getConfigString("options"));
        logger.info("Database: " + endpointConfig.getConfigString("database"));
        logger.info("Collection: " + endpointConfig.getConfigString("collection"));

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


    /*
        // resource_id
    private String id;
    // created
    private long created;
    // mtime
    private long mtime;
    // epoch
    private long epoch;
    // last_hash
    private String lastHash;
    // last_seen
    private long lastSeen;
    // data_size
    private long dataSize;
    // data_type
    private String dataType;
    // performance_tier
    private String performanceTier;
    // meta_map
    private Map<String, Object> metaMap = new HashMap<>();
     */

    private static Document fromResource(Resource resource){
        Document document = new Document();
        document.put("resource_id", resource.getId());
        document.put("created", resource.getCreated());
        document.put("mtime", resource.getMtime());
        document.put("epoch", resource.getEpoch());
        document.put("last_hash", resource.getLastHash());
        document.put("last_seen", resource.getLastSeen());
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
                document.getString("last_hash"),
                document.getLong("last_seen"),
                document.getLong("data_size"),
                document.getString("data_type"),
                document.getString("performance_tier"),
                document.get("meta_map", Document.class)
        );
    }

    @Override
    public String createResource(String performanceTier, long dataSize, String dataType, Map<String, Object> metaMap) {
        logger.trace("Creating resource!");
        MongoDatabase mdb = mongoClient.getDatabase(endpointConfig.getConfigString("database"));
        MongoCollection<Document> mcol = mdb.getCollection(endpointConfig.getConfigString("collection"));

        Resource resource = new Resource();
        resource.setId(UUID.randomUUID().toString().replace("-", ""))
                .setCreated(Instant.now().toEpochMilli())
                .setDataSize(dataSize)
                .setDataType(dataType)
                .setPerformanceTier(performanceTier)
                .setMeta(metaMap);

        logger.trace(String.format("Inserting resource %s into the database...",resource.getId()));
        mcol.insertOne(fromResource(resource));
        return resource.getId();
    }

    @Override
    public String createResource(ResourceRequest resourceRequest) throws Exception {

        logger.trace("Creating resource!");
        MongoDatabase mdb = mongoClient.getDatabase(endpointConfig.getConfigString("database"));
        MongoCollection<Document> mcol = mdb.getCollection(endpointConfig.getConfigString("collection"));

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
    public boolean updateResource(Resource resource) {
        logger.trace(String.format("Updating resource: %s", resource.getId()));
        BasicDBObject query = new BasicDBObject();
        query.put("uuid", resource.getId());
        return mongoCollection.updateOne(query, fromResource(resource)).wasAcknowledged();
    }

    @Override
    public Resource getResource(String resourceId) {
        logger.trace(String.format("Getting resource: %s", resourceId));
        BasicDBObject query = new BasicDBObject();
        query.put("uuid", resourceId);
        Document resourceObject = mongoCollection.find(query).first();
        return toResource(resourceObject);
    }

    @Override
    public boolean deleteResource(String resourceId) {
        logger.trace(String.format("Deleting resource: %s", resourceId));
        BasicDBObject query = new BasicDBObject();
        query.put("uuid", resourceId);
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
        query.put("uuid", resourceId);
        Document resourceObject = mongoCollection.find(query).first();
        return resourceObject.get("data", Binary.class).getData();
    }

    public byte[] readData(String resourceId, long offset, long length){
        byte[] dataBytes = readData(resourceId);
        // TODO: THIS STUFF
        return dataBytes;
    }

    @Override
    public boolean isReady() {
        ListCollectionsIterable<Document> mongoCollectionlist = mongoDatabase.listCollections()
                .maxTime(60, TimeUnit.SECONDS);

        // Here we're checking to make sure we return something. If we do then we know the connection is good.
        for(Document document : mongoCollectionlist){
            return true;
        }
        return false;
    }

}
