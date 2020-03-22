package stitch.aggregator;

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

public class AggregatorMain {

    static final Logger logger = Logger.getLogger(AggregatorMain.class);
    private static Map<String,Thread> aggregatorThreads = new HashMap<>();

    public static void main(String[] args) throws Exception {

        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);

        ConfigStore configStore = ConfigStore.loadConfigStore();

        // Get the id of the ResourceStoreProvider we'd like to start.
        String aggregatorId = null;
        for(int i = 0; i < args.length; i++) {
            if(args[i].equals("--id")){
                aggregatorId = args[i+1];
                break;
            }
        }

        // Create the map that contains the fields we'll use to find out aggregators.
        Map<String, String> aggregatorQueryAttributes = new HashMap();
        aggregatorQueryAttributes.put("type", ConfigItemType.AGGREGATOR.toString());

        // If we've specified an aggregator we'll add it to the filter.
        if(aggregatorId != null) {
            aggregatorQueryAttributes.put("uuid", aggregatorId);
        }

        // Start all the aggregators we've found.
        for (ConfigItem aggregatorConfig :configStore.getConfigItemsByAttributes(aggregatorQueryAttributes)) {
            startAggregatorServer(aggregatorConfig);
        }

        logger.info("All Aggregators started! Waiting for requests...");
        while (true){
            Thread.sleep(5000);
        }
    }
    private static void startAggregatorServer(ConfigItem config) throws IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        AggregatorServer aggregatorServer = new AggregatorServer(config);
        Thread aggregatorThread = new Thread(aggregatorServer);
        aggregatorThread.setName(config.getConfigName());
        aggregatorThreads.put(config.getConfigId(), aggregatorThread);
        aggregatorThread.start();
        logger.info(String.format("AggregatorCallable %s started!", config.getConfigId()));
    }
}
