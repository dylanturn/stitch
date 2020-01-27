package stitch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import stitch.aggregator.AggregatorServer;
import stitch.aggregator.redisearch.RedisAggregatorServer;
import stitch.aggregator.Aggregator;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.item.ConfigItemType;
import stitch.util.configuration.store.ConfigStore;

import java.util.HashMap;
import java.util.Map;

public class AggregatorMain {

    static final Logger logger = Logger.getLogger(AggregatorMain.class);
    private static Map<String,Aggregator> aggregatorHash = new HashMap<>();
    private static Map<String,Thread> aggregatorThreads = new HashMap<>();

    public static void main(String[] args) throws Exception {

        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);
        ConfigStore configStore = ConfigStore.loadConfigStore();

        // For right now we're just going to choose the first Aggregator
        ConfigItem aggregatorConfig = configStore.listConfigByItemType(ConfigItemType.AGGREGATOR).get(0);

        AggregatorServer aggregator = new RedisAggregatorServer(aggregatorConfig);
        Thread aggregatorThread = new Thread(aggregator);
        aggregatorThreads.put(aggregatorConfig.getConfigId(), aggregatorThread);
        aggregatorHash.put(aggregatorConfig.getConfigId(), aggregator);
        aggregatorThread.start();

        logger.info("Dataprovider started. Waiting for requests...");
        while (true){
            Thread.sleep(5000);
        }
    }
}
