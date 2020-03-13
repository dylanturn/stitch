package stitch.datastore;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.item.ConfigItemType;
import stitch.util.configuration.store.ConfigStore;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class DataStoreMain {

    private static final Logger logger = Logger.getLogger(DataStoreMain.class);
    private static Map<String,Thread> datastoreThreads = new HashMap<>();

    public static void main(String[] args) throws Exception {

        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);
        ConfigStore configStore = ConfigStore.loadConfigStore();

        // Get the id of the ResourceStoreProvider we'd like to start.
        String dataStoreId = null;
        for(int i = 0; i < args.length; i++) {
            if(args[i].equals("--id")){
                dataStoreId = args[i+1];
                break;
            }
        }

        if(dataStoreId == null) {
            // Create an instance of each ResourceStoreProvider.
            for (ConfigItem configItem : configStore.listConfigByItemType(ConfigItemType.DATASTORE)) {
                startDataStoreServer(configItem);
            }
        } else {
            logger.info("Starting ResourceStoreProvider with Id: " + dataStoreId);
            startDataStoreServer(ConfigStore.loadConfigStore().getConfigItemById(dataStoreId));
        }

        logger.info("Dataprovider started. Waiting for requests...");
        while (true){
            Thread.sleep(5000);
        }
    }
    private static void startDataStoreServer(ConfigItem config) throws ClassNotFoundException, InvocationTargetException, InstantiationException, NoSuchMethodException, IllegalAccessException {
        DataStoreServer dataStore = new DataStoreServer(config);
        Thread datastoreThread = new Thread(dataStore);
        datastoreThread.setName(config.getConfigName());
        datastoreThreads.put(config.getConfigId(), datastoreThread);
        datastoreThread.start();
        logger.info("ResourceStoreProvider instance started!!!");
    }
}
