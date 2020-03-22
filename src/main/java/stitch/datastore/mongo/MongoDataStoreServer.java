package stitch.datastore.mongo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import org.apache.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.conversions.Bson;
import org.bson.types.Code;
import org.slf4j.LoggerFactory;
import stitch.datastore.sqlquery.conditions.QueryCondition;
import stitch.datastore.sqlquery.conditions.QueryConditionGroup;
import stitch.datastore.sqlquery.SearchQuery;
import stitch.datastore.resource.Resource;
import stitch.datastore.resource.ResourceRequest;
import stitch.datastore.resource.ResourceStoreProvider;
import stitch.datastore.sqlquery.conditions.QueryConditionGroupType;
import stitch.util.configuration.item.ConfigItem;

import javax.print.Doc;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.time.DayOfWeek;
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
        //((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);
        this.providerConfig = providerConfig;
        try {
            dsURI = String.format("%s://%s:%s@%s/%s?%s",
                    providerConfig.getConfigString("protocol"),
                    providerConfig.getConfigString("username"),
                    providerConfig.getConfigString("password"),
                    providerConfig.getConfigString("host"),
                    providerConfig.getConfigString("database"),
                    providerConfig.getConfigString("options"));
            MongoClientURI mongoClientURI = new MongoClientURI(dsURI);
            mongoClient = new MongoClient(mongoClientURI);
            mongoDatabase = mongoClient.getDatabase(providerConfig.getConfigString("database"));
            this.mongoCollection = mongoDatabase.getCollection(providerConfig.getConfigString("collection"));
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
        resource.setId(resourceRequest.getId())
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
    public boolean updateResource(ResourceRequest resourceRequest) throws Exception {
        logger.trace(String.format("Updating resource: %s", resourceRequest.getId()));
        BasicDBObject query = new BasicDBObject();
        // Get the resource we want to update.
        Resource resource = getResource(resourceRequest.getId());
        if(resourceRequest.getEpoch() == resource.getEpoch()) {
            resource = resource.updateResource(resourceRequest);
            query.put("resource_id", resourceRequest.getId());
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
    public ArrayList<Resource> findResources(SearchQuery searchQuery) throws Exception {

        System.out.println("Select Clauses");
        for(String clause : searchQuery.getSelectClause()){
            System.out.println(clause);
        }

        System.out.println("Where Clauses");
        for(QueryConditionGroup queryConditionGroup : searchQuery.getWhereConditions()){
            System.out.println("Where Condition Group");
            for(QueryCondition queryCondition : queryConditionGroup.getGroupConditions()){
                System.out.println("Field: " + queryCondition.getConditionField());
                System.out.println("Value: " + queryCondition.getConditionValue());
            }
        }

        BasicDBObject selectClause = new BasicDBObject();
        List<Bson> whereFilterGroup = new ArrayList<>();
        FindIterable<Document> foundDocuments;

        for(String clause : searchQuery.getSelectClause()){
            if(clause.equals("*")){
                selectClause = null;
                break;
            } else {
                selectClause.put(clause, 1);
            }
        }

        // Construct groups of filters needed for the Query.
        // Will probably end up looking something like this:
        // (foo=bar AND boo=baz) AND (snook=sunk OR snook=dunk)
        for(QueryConditionGroup queryConditionGroup : searchQuery.getWhereConditions()){

            // If this condition group is of an OR type we create a group or OR filters.
            if(queryConditionGroup.getQueryConditionGroupType().equals(QueryConditionGroupType.OR)){
                List<Bson> orFilterGroup = new ArrayList<>();
                for(QueryCondition queryCondition : queryConditionGroup.getGroupConditions()){
                    orFilterGroup.add(parseQueryCondition(queryCondition));
                }
                whereFilterGroup.add(Filters.or(orFilterGroup.toArray(new Bson[0])));


            // If this condition group is NOT of an OR type we made a group of AND filters.
            } else {
                List<Bson> andFilterGroup = new ArrayList<>();
                for(QueryCondition queryCondition : queryConditionGroup.getGroupConditions()){
                    andFilterGroup.add(parseQueryCondition(queryCondition));
                }
                whereFilterGroup.add(Filters.and(andFilterGroup.toArray(new Bson[0])));
            }
        }

        Bson queryFilter = Filters.and(whereFilterGroup.toArray(new Bson[0]));

        if(selectClause == null) {
            foundDocuments = mongoCollection.find(queryFilter);
        } else {
            foundDocuments = mongoCollection.find(queryFilter).projection(selectClause);
        }

        ArrayList<Resource> resources = new ArrayList<>();
        for(Document document : foundDocuments){
            resources.add(toResource(document));
        }

        return resources;
    }

    private Bson parseQueryCondition(QueryCondition queryCondition) throws ParseException {
        switch(queryCondition.getQueryConditionOperator()){
            case EQ:
                return Filters.eq(queryCondition.getConditionField(), queryCondition.getConditionValue());
            case NE:
                return Filters.ne(queryCondition.getConditionField(), queryCondition.getConditionValue());
            case GTE:
                return Filters.gte(queryCondition.getConditionField(), queryCondition.getConditionValue());
            case GT:
                return Filters.gt(queryCondition.getConditionField(), queryCondition.getConditionValue());
            case LTE:
                return Filters.lte(queryCondition.getConditionField(), queryCondition.getConditionValue());
            case LT:
                return Filters.lt(queryCondition.getConditionField(), queryCondition.getConditionValue());
            default:
                throw new ParseException(String.format("Unable to parse: %s", queryCondition.getQueryConditionOperator()), 0);
        }
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
        updateResource(
                new ResourceRequest()
                        .setId(resourceId)
                        .setEpoch(curResource.getEpoch())
                        .setDataSize(dataBytes.length));
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
