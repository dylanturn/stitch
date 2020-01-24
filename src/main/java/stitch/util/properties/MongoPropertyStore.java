package stitch.util.properties;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class MongoPropertyStore implements PropertyStore {

    static final Logger logger = Logger.getLogger(MongoPropertyStore.class);

    private Properties properties = new Properties();
    private String propFileName = "application.properties";
    private String propPrefix = "stitch_ps";

    private  MongoClient mongoClient;
    private MongoCollection<Document> mongoCollection;
    private String mongoAdminDB;
    private String mongoEndpointCollection;

    public MongoPropertyStore() {
        logger.trace("Loading settings from MongoDB");
        try {
            MongoClientURI mongoURI = new MongoClientURI(loadMongoStringURI());
            mongoClient = new MongoClient(mongoURI);
            MongoDatabase mongoDatabase = mongoClient.getDatabase(this.mongoAdminDB);
            this.mongoCollection = mongoDatabase.getCollection(this.mongoEndpointCollection);
        } catch(Exception error){
            logger.error("Failed to load the collection!");
            logger.error(error);
        }
    }

    private String loadMongoStringURI(){
        logger.trace("Loading Application Properties...");
        properties = new Properties();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        if (inputStream != null) {
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                logger.error("Failed to load application properties input stream", e);
                e.printStackTrace();
                System.exit(100);
            }

            // Having this here feels weird. Need to redo.
            // TODO: replace the jank with less jank.
            this.mongoAdminDB = properties.getProperty(String.format("%s_database",this.propPrefix));
            this.mongoEndpointCollection = properties.getProperty(String.format("%s_collection",this.propPrefix));

            String protocol = properties.getProperty(String.format("%s_protocol",this.propPrefix));
            String username = properties.getProperty(String.format("%s_user",this.propPrefix));
            String password = properties.getProperty(String.format("%s_password",this.propPrefix));
            String endpoint = properties.getProperty(String.format("%s_endpoint",this.propPrefix));
            String options = properties.getProperty(String.format("%s_options",this.propPrefix));
            String queryString = String.format("%s://%s:%s@%s/%s?%s", protocol, username, password, endpoint, this.mongoAdminDB, options);
            logger.trace("Connecting using: " + queryString);
            return queryString;

        } else {
            logger.error("property file '" + propFileName + "' not found in the classpath");
            System.exit(100);
        }
        logger.trace("Failed to load application properties");
        System.exit(100);
        return null;
    }

    private Document getDocument(String sectionName) {
        return mongoCollection.find(Filters.eq("name", sectionName)).first();
    }

    @Override
    public void writeKey(String keyName, String keyValue) {}

    @Override
    public String readKey(String keyName) {
        return null;
    }

    @Override
    public String getString(String sectionName, String propertyName) {
        return getDocument(sectionName).getString(propertyName);
    }

    @Override
    public int getInt(String sectionName, String propertyName) {
        return getDocument(sectionName).getInteger(propertyName);
    }

    @Override
    public boolean getBool(String sectionName, String propertyName) {
        return getDocument(sectionName).getBoolean(propertyName);
    }

    @Override
    public String getSecret(String sectionName, String secretName, String secretKey, String secretSalt) {

        String encryptedSecret = getDocument(sectionName).getString(secretName);

        // TODO: Decryption magic
        String decryptedSecret = encryptedSecret;

        return decryptedSecret;
    }

    @Override
    public ArrayList<String> listDataStores() {
        ArrayList<String> dataStoreIdList = new ArrayList<>();
        FindIterable<Document> dataStoreDocuments = mongoCollection.find(Filters.eq("type", "datastore"));
        for(Document dataStoreDocument : dataStoreDocuments){
            dataStoreIdList.add(dataStoreDocument.getString("uuid"));
        }
        return dataStoreIdList;
    }

    @Override
    public StitchProperty getDataStore(String dataStoreId) throws ClassNotFoundException {
        List<String> dataStoreKeyList = new ArrayList<>();
        dataStoreKeyList.add("transport");
        dataStoreKeyList.add("protocol");
        dataStoreKeyList.add("host");
        dataStoreKeyList.add("port");
        dataStoreKeyList.add("username");
        dataStoreKeyList.add("password");
        dataStoreKeyList.add("options");
        dataStoreKeyList.add("database");
        dataStoreKeyList.add("collection");
        dataStoreKeyList.add("aggregator");
        Document dataStoreDocument = mongoCollection.find(Filters.eq("uuid",dataStoreId)).first();
        return new StitchProperty()
                .setObjectId(dataStoreId)
                .setObjectName(dataStoreDocument.getString("name"))
                .setObjectType(dataStoreDocument.getString("type"))
                .setObjectClass(dataStoreDocument.getString("class"))
                .addAllObjectsByKeys(dataStoreKeyList, dataStoreDocument);
    }

    @Override
    public ArrayList<String> listAggregators() {
        ArrayList<String> aggregatorIdList = new ArrayList<>();
        FindIterable<Document> aggregatorDocuments = mongoCollection.find(Filters.eq("type", "aggregator"));
        for(Document dataStoreDocument : aggregatorDocuments){
            aggregatorIdList.add(dataStoreDocument.getString("uuid"));
        }
        return aggregatorIdList;
    }

    @Override
    public StitchProperty getAggregator(String aggregatorId) throws ClassNotFoundException {
        List<String> aggregatorKeyList = new ArrayList<>();
        aggregatorKeyList.add("transport");
        aggregatorKeyList.add("indexPrefix");
        aggregatorKeyList.add("host");
        aggregatorKeyList.add("port");
        aggregatorKeyList.add("timeout");
        aggregatorKeyList.add("poolSize");
        aggregatorKeyList.add("password");
        Document aggregatorDocument = mongoCollection.find(Filters.eq("uuid",aggregatorId)).first();
        return new StitchProperty()
                .setObjectId(aggregatorId)
                .setObjectName(aggregatorDocument.getString("name"))
                .setObjectType(aggregatorDocument.getString("type"))
                .setObjectClass(aggregatorDocument.getString("class"))
                .addAllObjectsByKeys(aggregatorKeyList, aggregatorDocument);
    }

    @Override
    public ArrayList<String> listTransports() {
        ArrayList<String> transportIdList = new ArrayList<>();
        FindIterable<Document> transportDocuments = mongoCollection.find(Filters.eq("type", "aggregator"));
        for(Document dataStoreDocument : transportDocuments){
            transportIdList.add(dataStoreDocument.getString("uuid"));
        }
        return transportIdList;
    }

    @Override
    public StitchProperty getTransport(String transportId) {
        List<String> transportKeyList = new ArrayList<>();
        transportKeyList.add("client_class");
        transportKeyList.add("server_class");
        transportKeyList.add("host");
        transportKeyList.add("port");
        transportKeyList.add("username");
        transportKeyList.add("password");
        transportKeyList.add("exchange");
        Document transportDocument = mongoCollection.find(Filters.eq("uuid",transportId)).first();
        return new StitchProperty()
                .setObjectId(transportDocument.getString("uuid"))
                .setObjectName(transportDocument.getString("name"))
                .setObjectType(transportDocument.getString("type"))
                .addAllObjectsByKeys(transportKeyList, transportDocument);

/*

        {
    "_id": {
        "$oid": "5e2af5ea1c9d4400006ff1a0"
    },
    "uuid": "95a6495db3ef46c5aa98b428127c2cd4",
    "name": "amqp",
    "type": "transport",
    "class": "stitch.rpc.transport.amqp",
    "client_class": "stitch.rpc.transport.amqp.AMQPClient",
    "server_class": "stitch.rpc.transport.amqp.AMQPServer",
    "host": "wildboar.rmq.cloudamqp.com",
    "port": {
        "$numberInt": "5672"
    },
    "username": "kmtdjhog",
    "password": "OKjL910CrnIclfs6MS8u211yRziIU_f7",
    "exchange": "stitch"
}

*/
    }


    @Override
    public void close(){
        this.mongoClient.close();
    }
}


