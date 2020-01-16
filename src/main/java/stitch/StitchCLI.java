package stitch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import stitch.aggregator.AggregatorClient;
import stitch.amqp.rpc.RPCStats;
import stitch.util.Resource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Callable;

@Command(name = "stitchcli", mixinStandardHelpOptions = true, version = "stitchcli 0.1",
        description = "Interacts with an aggregator via commandline")
class StitchCLI implements Callable<Integer> {

    static final Logger logger = Logger.getLogger(StitchCLI.class);
    private AggregatorClient aggregatorClient;

    @Parameters(index = "0", description = "The action to perform.")
    private String action;

    @Parameters(index = "1", description = "The subject to perform the action against.")
    private String subject;

    @Option(names = {"-t", "--type"}, description = "The UUID of the aggregator to interact with")
    private String resourceType;

    @Option(names = {"-d", "--data"}, description = "The UUID of the aggregator to interact with")
    private String resourceData;

    @Option(names = {"-a", "--aggregatorId"}, description = "The UUID of the aggregator to interact with")
    private String aggregatorId;

    @Option(names = {"--query"}, description = "The UUID of the aggregator to interact with")
    private String query = "";

    @Option(names = {"-q", "--quiet"}, description = "Only prints the uuids of found or listed resources")
    private boolean quiet = false;

    @Option(names = {"-r", "--resourceId"}, description = "The UUID of the aggregator to interact with")
    private String resourceId = "";

    public static void main(String... args) {
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);
        int exitCode = new CommandLine(new StitchCLI()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {

        logger.trace("Loading Application Properties...");
        Properties properties = new Properties();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("~/.stitch.properties");
        if (inputStream != null) {
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                logger.error("Failed to load application properties input stream", e);
                e.printStackTrace();
                System.exit(100);
            }
        }

        try {
            if(aggregatorId == null){
                aggregatorId = properties.getProperty("aggregator");
            }

            aggregatorClient = new AggregatorClient(aggregatorId);

            switch (action) {
                case "list":
                    switch (subject){
                        case "resources":
                            listAndPrint();
                            break;
                        case "datastores":
                            listAndPrintDataStores();
                            break;
                    }

                case "find":
                    findAndPrint(query);
                    break;

                case "get":
                    getAndPrint(resourceId);
                    break;

                case "stats":
                    statsAndPrint();
                    break;

                case "new":
                    createResource(resourceType, resourceData);
                    break;

                case "delete":
                    deleteResource(resourceId);
                    break;
            }

        } catch (Exception e) {
            logger.error("Failed to connect to aggregator!", e);
            return 500;
        }
        return 0;
    }

    private void statsAndPrint(){
        System.out.println("STATS?!");
        RPCStats rpcStats = aggregatorClient.getRpcStats();
        System.out.println("Total Calls:   " + rpcStats.getTotalCalls());
        System.out.println("Success Calls: " + rpcStats.getTotalCalls());
        System.out.println("Failed Calls:  " + rpcStats.getTotalCalls());
    }

    /*

    DATASTORES

     */

    private void listAndPrintDataStores(){
        logger.trace("Execute list and print.");
        ArrayList<String> dataStoreArrayList = aggregatorClient.listDataStores();
        printDataStoreTable(dataStoreArrayList);
    }

    private void printDataStoreTable(ArrayList<String> dataStoreArrayList){
        for(String datastore : dataStoreArrayList){
            System.out.println(datastore);
        }
        /*if(!quiet) {
            System.out.println("DataStore Count: " + dataStoreArrayList.size());
            if(dataStoreArrayList.size() > 0) {
                String tableHeader = String.format("| %-36s | %-36s | %-10s | %-10s | %-20s |", "Resource ID", "DataStore ID", "Type", "Size", "Timestamp");
                System.out.println(tableHeader);
            }
        }
        for(Resource resource : resourceArrayList) {
            String uuid = resource.getUUID();
            String datastoreId = (String)resource.getMeta("datastoreId");
            String dataType = (String)resource.getMeta("data_type");
            int dataSize = resource.getMetaInt("data_size");
            long created = resource.getMetaLong("created");
            String tableBody = uuid;
            if(!quiet) {
                tableBody = String.format("| %36s | %36s | %10s | %10s | %20s |", uuid, datastoreId, dataType, dataSize, created);
            }
            System.out.println(tableBody);
        }*/
    }

    /*

    RESOURCES

     */

    private void listAndPrint(){
        logger.trace("Execute list and print.");
        ArrayList<Resource> resourceArrayList = aggregatorClient.listResources();
        printResourceTable(resourceArrayList);
    }

    private void printResourceTable(ArrayList<Resource> resourceArrayList){
        if(!quiet) {
            System.out.println("Resource Count: " + resourceArrayList.size());
            if(resourceArrayList.size() > 0) {
                String tableHeader = String.format("| %-36s | %-36s | %-10s | %-10s | %-20s |", "Resource ID", "DataStore ID", "Type", "Size", "Timestamp");
                System.out.println(tableHeader);
            }
        }
        for(Resource resource : resourceArrayList) {
            String uuid = resource.getUUID();
            String datastoreId = (String)resource.getMeta("datastoreId");
            String dataType = (String)resource.getMeta("data_type");
            int dataSize = resource.getMetaInt("data_size");
            long created = resource.getMetaLong("created");
            String tableBody = uuid;
            if(!quiet) {
                tableBody = String.format("| %36s | %36s | %10s | %10s | %20s |", uuid, datastoreId, dataType, dataSize, created);
            }
            System.out.println(tableBody);
        }
    }

    private void getAndPrint(String resourceId){
        Resource resource = aggregatorClient.getResource(resourceId);
        System.out.println("UUID:      " + resource.getUUID());
        System.out.println("DataStore: " + resource.getMeta("datastoreId"));
        System.out.println("Type:      " + resource.getMeta("data_type"));
        System.out.println("Size:      " + resource.getMeta("data_size"));
        System.out.println("Timestamp: " + resource.getMeta("created"));
        System.out.println("Data:      " + new String(resource.getData()));
    }

    private void createResource(String resourceType, String resourceData){
        aggregatorClient.createResource(new Resource(resourceType, resourceData.getBytes()));
    }

    private void deleteResource(String resourceId){
        aggregatorClient.deleteResource(resourceId);
    }

    private void findAndPrint(String query){
        ArrayList<Resource> resourceArrayList = aggregatorClient.findResources(query);
        printResourceTable(resourceArrayList);
    }
}