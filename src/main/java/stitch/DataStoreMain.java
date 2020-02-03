package stitch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import stitch.datastore.DataStoreCallable;
import stitch.datastore.DataStoreFactory;
import stitch.datastore.DataStoreServer;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.item.ConfigItemType;
import stitch.util.configuration.store.ConfigStore;

import java.util.HashMap;
import java.util.Map;

public class DataStoreMain {

    private static final Logger logger = Logger.getLogger(DataStoreMain.class);
    private static Map<String,DataStoreCallable> providerHash = new HashMap<>();
    private static Map<String,Thread> providerThreads = new HashMap<>();

    public static void main(String[] args) throws Exception {

        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);
        ConfigStore configStore = ConfigStore.loadConfigStore();

        // Get the id of the DataStoreCallable we'd like to start.
        String dataStoreId = null;
        for(int i = 0; i < args.length; i++) {
            if(args[i].equals("--id")){
                dataStoreId = args[i+1];
                break;
            }
        }


        if(dataStoreId == null) {
            // Create an instance of each DataStoreCallable.
            for (ConfigItem configItem : configStore.listConfigByItemType(ConfigItemType.DATASTORE)) {
                DataStoreServer dataStore = DataStoreFactory.createDataStore(configItem.getConfigId());
                Thread providerThread = new Thread(dataStore);
                providerThreads.put(configItem.getConfigId(), providerThread);
                providerHash.put(configItem.getConfigId(), dataStore);
                //providerThread.setName(String.format("DataStoreCallable-%s", configItem.getConfigId()));
                providerThread.start();
                logger.info("DataStoreCallable instance started!!!");
            }
        } else {
            logger.info("Starting DataStoreCallable with Id: " + dataStoreId);
            // Start the DataStoreCallable we specified.
            DataStoreServer dataStore = DataStoreFactory.createDataStore(dataStoreId);
            Thread providerThread = new Thread(dataStore);
            providerThreads.put(dataStoreId, providerThread);
            providerHash.put(dataStoreId, dataStore);
            //providerThread.setName(String.format("DataStoreCallable-%s", dataStoreId));
            providerThread.start();
            logger.info("DataStoreCallable instance started!!!");
        }

        logger.info("Dataprovider started. Waiting for requests...");
        while (true){
            Thread.sleep(5000);
        }
    }
}
