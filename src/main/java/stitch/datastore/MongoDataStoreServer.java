package stitch.datastore;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;
import stitch.amqp.HealthReport;
import stitch.resource.Resource;

import java.util.*;

import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class MongoDataStoreServer extends DataStoreServer {

    static final Logger logger = Logger.getLogger(MongoDataStoreServer.class);

    private String dsURI;
    private String dsProtocol = providerArgs.getString("protocol");
    private String dsHost = providerArgs.getString("host");
    private int dsPort = providerArgs.getInteger("port");
    private String dsUsername = providerArgs.getString("username");
    private String dsPassword = providerArgs.getString("password");
    private String dsOptions = providerArgs.getString("options");
    private String database = providerArgs.getString("database");
    private String collection = providerArgs.getString("collection");

    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Document> mongoCollection;

    public MongoDataStoreServer(Document providerArgs) throws Exception {
        super(providerArgs);
    }

    private static Document fromResource(Resource resource) {
        return fromResource(resource, true);
    }

    private static Document fromResource(Resource resource, boolean includeData){
        Document document = new Document();
        Document metaDocument = new Document();
        document.put("uuid", resource.getUUID());
        metaDocument.putAll(resource.getMetaMap());
        document.put("meta", metaDocument);
        if(includeData) {
            document.put("data", resource.getData());
        } else {
            document.put("data", new byte[0]);
        }
        return document;
    }

    private static Resource toResource(Document document) {
        return toResource(document, true);
    }

    private static Resource toResource(Document document, boolean includeData){
        String uuid = document.getString("uuid");
        Document metaDocument = document.get("meta", Document.class);
        HashMap<String, Object> metaMap = new HashMap<>();
        for(Map.Entry<String, Object> entry : metaDocument.entrySet()) {
            metaMap.put(entry.getKey(), entry.getValue());
        }

        if(includeData){
            return new Resource(uuid, metaMap, document.get("data", Binary.class).getData());
        } else {
            return new Resource(uuid, metaMap, null);
        }
    }

    @Override
    public void connect() {

        logger.info("Start a new DataStore instance...");
        logger.info("UUID:  " + getId());
        logger.info("Class: " + getStoreClass());
        logger.info("Type: " + getStoreType());
        logger.info("Host: " + dsHost);
        logger.info("Options: " + dsOptions);
        logger.info("Database: " + database);
        logger.info("Collection: " + collection);

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

    // TODO: Make sure the node is actually healthy
    @Override
    public void reportHealth(HealthReport healthReport) {}

    @Override
    public String createResource(Resource resource) {
        try {
            logger.info(String.format("Creating resource: %s", resource.getUUID()));
            MongoDatabase mdb = mongoClient.getDatabase(database);
            MongoCollection<Document> mcol = mdb.getCollection(collection);
            mcol.insertOne(fromResource(resource));
            return resource.getUUID();
        } catch(Exception error){
            logger.error("Mongo Update Failed!", error);
            return null;
        }
    }

    @Override
    public boolean updateResource(Resource resource) {
        logger.trace(String.format("Updating resource: %s", resource.getUUID()));
        BasicDBObject query = new BasicDBObject();
        query.put("uuid", resource.getUUID());
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
        return this.listResources(true);
    }

    @Override
    public ArrayList<Resource> listResources(boolean includeData) {
        logger.trace("Listing Resources");
        ArrayList<Resource> resourceList = new ArrayList<>();
        FindIterable<Document> foundDocuments;
        if(includeData){
            foundDocuments = mongoCollection.find();
        } else {
            foundDocuments = mongoCollection.find().projection(fields(include("uuid", "meta")));
        }
        for(Document document : foundDocuments){
            resourceList.add(toResource(document, includeData));
        }
        return resourceList;
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down aggregator client...");
    }
}
