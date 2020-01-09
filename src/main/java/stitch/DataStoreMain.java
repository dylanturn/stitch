package stitch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import stitch.datastore.BaseDataStore;
import stitch.datastore.DataStore;
import stitch.datastore.MongoDataStore;
import stitch.util.properties.MongoPropertyStore;
import stitch.util.properties.PropertyStore;

import java.util.HashMap;
import java.util.Map;

public class DataStoreMain {

    private static final Logger logger = Logger.getLogger(DataStoreMain.class);
    private static Map<String,DataStore> providerHash = new HashMap<>();
    private static Map<String,Thread> providerThreads = new HashMap<>();

    public static void main(String[] args) throws Exception {

        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);

        // Get Stitch_DB connection settings.
        PropertyStore propertyStore = new MongoPropertyStore();
        String secretKey = "";
        String secretSalt = "";
        String protocol = propertyStore.getString("stitch_discovery", "protocol");
        String host = propertyStore.getString("stitch_discovery", "host");
        String username = propertyStore.getString("stitch_discovery", "username");
        String password = propertyStore.getSecret("stitch_discovery", "password", secretKey, secretSalt);
        String options = propertyStore.getString("stitch_discovery", "options");
        String database = propertyStore.getString("stitch_discovery", "database");
        String collection = propertyStore.getString("stitch_discovery", "collection");
        propertyStore.close();

        // Connect to Stitch_DB to get all the providers.
        String dbURI = String.format("%s://%s:%s@%s/%s?%s", protocol, username, password, host, database, options);
        logger.info("Connecting to Stitch provider DB: " + dbURI);
        MongoClient mongoClient = new MongoClient(new MongoClientURI(dbURI));
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
        FindIterable<Document> providers = mongoCollection.find(Filters.eq("type", "data_provider"));

        // Create an instance of each provider.
        for(Document document : providers){
            BaseDataStore dataStore = new MongoDataStore(document);
            Thread providerThread = new Thread(dataStore);
            providerThreads.put(document.getString("uuid"), providerThread);
            providerHash.put(document.getString("uuid"), dataStore);
            providerThread.start();
            logger.info("DataStore instance started!!!");
        }

        // Now that all the providers are started we can close this connection.
        mongoClient.close();
        logger.info("Dataprovider started. Waiting for requests...");
        while (true){
            Thread.sleep(5000);
        }
    }
}
