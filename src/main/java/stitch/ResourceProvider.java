package stitch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import stitch.aggregator.BaseAggregator;
import stitch.aggregator.RedisAggregator;
import stitch.aggregator.Aggregator;
import stitch.util.properties.MongoPropertyStore;
import stitch.util.properties.PropertyStore;

import java.util.HashMap;
import java.util.Map;

public class ResourceProvider {

    static final Logger logger = Logger.getLogger(ResourceProvider.class);
    private static Map<String,Aggregator> aggregatorHash = new HashMap<>();
    private static Map<String,Thread> aggregatorThreads = new HashMap<>();

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

        MongoClientURI mongoURI = new MongoClientURI(dbURI);
        MongoClient mongoClient = new MongoClient(mongoURI);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        // Get a list of the data providers and aggrigators.
        Iterable<Document> providers = mongoCollection.find(Filters.eq("type", "data_provider"));
        Iterable<Document> aggregators = mongoCollection.find(Filters.eq("type", "data_aggregator"));

        // Create and start an instance of each stitch.aggregator.
        for(Document document : aggregators){
            BaseAggregator aggregator = new RedisAggregator(document, providers);
            Thread aggregatorThread = new Thread(aggregator);
            aggregatorThreads.put(document.getString("uuid"), aggregatorThread);
            aggregatorHash.put(document.getString("uuid"), aggregator);
            aggregatorThread.start();
            logger.info("Aggregator instance started!!!");
        }

        // Now that all the providers are started we can close this connection.
        mongoClient.close();
        logger.info("Dataprovider started. Waiting for requests...");
        while (true){
            Thread.sleep(5000);
        }
    }
}
