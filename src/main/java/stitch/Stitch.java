package stitch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import stitch.aggregator.AggregatorClient;
import stitch.util.Resource;

public class Stitch {

    static final Logger logger = Logger.getLogger(Stitch.class);

    public static void main(String[] args) throws Exception {

        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);


        // Test the DataStore AMQP listener and stuff.....

        AggregatorClient aggregatorClient = new AggregatorClient("7d1247d5-1e15-45ed-8987-27efd105efc2");

        // Print the metadata for all the hosted resources.
        logger.info("Print the metadata for all the hosted resources.");
        for (Resource resource : aggregatorClient.listResources()) {
            System.out.println("Resource UUID: " + resource.getUUID());
            System.out.println("Resource Size: " + resource.getMeta("data_size"));
        }

    /*
        // Create a new resource
        logger.info("Create a new resource.");
        Resource newResource = new Resource("json", "{\"foo\": \"bar\"}".getBytes());
        String resourceId = dataStoreClient.createResource(newResource);
        logger.info("Created new resource: " + resourceId);

        // Print the metadata for all the hosted resources.
        logger.info("Print the metadata for all the hosted resources.");
        for (Resource resource : dataStoreClient.listResources()) {
            System.out.println("Resource UUID: " + resource.getUUID());
            System.out.println("Resource Size: " + resource.getMeta("data_size"));
        }

*/
    }
}
