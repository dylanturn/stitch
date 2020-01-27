package stitch.datastore.mongo;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;
import stitch.datastore.DataStoreServer;
import stitch.resource.Resource;
import stitch.util.configuration.item.ConfigItem;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

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

    @Override
    public String createResource(Resource resource) {
        try {
            logger.info(String.format("Creating resource: %s", resource.getUUID()));
            MongoDatabase mdb = mongoClient.getDatabase(endpointConfig.getConfigString("database"));
            MongoCollection<Document> mcol = mdb.getCollection(endpointConfig.getConfigString("collection"));
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

    // TODO: Implement some kind of resource search logic.
    @Override
    public ArrayList<Resource> findResources(String filter) {
        return listResources();
    }

}