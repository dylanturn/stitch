package stitch.util.configuration.store.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.item.ConfigItemType;
import stitch.util.configuration.store.ConfigStore;

import java.util.*;

public class MongoPropertyStore extends ConfigStore {

    static final Logger logger = Logger.getLogger(MongoPropertyStore.class);

    private  MongoClient mongoClient;
    private MongoCollection<Document> mongoCollection;
    private String mongoAdminDB;
    private String mongoEndpointCollection;

    public MongoPropertyStore() {
        this.mongoAdminDB = this.readProperty("database");
        this.mongoEndpointCollection = readProperty("collection");

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
        String protocol = readProperty("protocol");
        String username = readProperty("user");
        String password = readProperty("password");
        String endpoint = readProperty("endpoint");
        String options = readProperty("options");
        String queryString = String.format("%s://%s:%s@%s/%s?%s", protocol, username, password, endpoint, this.mongoAdminDB, options);
        return queryString;
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
    public ConfigItem getConfigItemById(String configItemId) {
        return getConfigItemsByAttribute("uuid", configItemId).get(0);
    }

    @Override
    public ConfigItem getConfigItemByName(ConfigItemType configItemType) {
        return getConfigItemsByAttribute("name", configItemType.toString()).get(0);
    }

    @Override
    public List<ConfigItem> listConfigByItemType(ConfigItemType configItemType) {
        return getConfigItemsByAttribute("type", configItemType.toString());
    }

    @Override
    public List<ConfigItem> getConfigItemsByAttribute(String attributeName, String attributeValue) {
        ArrayList<ConfigItem> configItemArrayList = new ArrayList<>();
        FindIterable<Document> configItemDocuments = mongoCollection.find(Filters.eq(attributeName, attributeValue));
        for(Document configItemDocument : configItemDocuments){
            configItemArrayList.add(new ConfigItem().addAllConfigProperties(configItemDocument));
        }
        return configItemArrayList;
    }

    @Override
    public List<ConfigItem> getConfigItemsByAttributes(Map<String,String> attributes) {
        ArrayList<ConfigItem> configItemArrayList = new ArrayList<>();
        List<Bson> bsonFilters = new ArrayList<>();
        for(String key : attributes.keySet()){
            bsonFilters.add(Filters.eq(key, attributes.get(key)));
        }
        FindIterable<Document> configItemDocuments = mongoCollection.find(Filters.and(bsonFilters));
        for(Document configItemDocument : configItemDocuments){
            configItemArrayList.add(new ConfigItem().addAllConfigProperties(configItemDocument));
        }
        return configItemArrayList;
    }


}


