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
import stitch.datastore.DataStoreInfo;
import stitch.datastore.DataStoreStatus;
import stitch.resource.Resource;
import stitch.util.EndpointStatus;

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
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("stitch").setLevel(Level.TRACE);
        int exitCode = new CommandLine(new StitchCLI()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {

        logger.trace("Loading Application Properties...");
        Properties properties = new Properties();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("~/.stitch.configuration");
        if (inputStream != null) {
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                logger.error("Failed to load application configuration input stream", e);
                e.printStackTrace();
                System.exit(100);
            }
        }

        try {
            if(aggregatorId == null){
                aggregatorId = properties.getProperty("aggregator");
            }

            logger.trace("Connecting to aggregator: " + aggregatorId);
            // TODO: Fix this
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

    /*

    DATASTORES

     */

    private void listAndPrintDataStores() throws ClassNotFoundException {
        logger.trace("Execute list and print.");
        logger.trace("Is the RPC Client Connected: " + aggregatorClient.isRpcConnected());
        logger.trace("Is the RPC Client Ready: " + aggregatorClient.isRpcReady());
        printDataStoreTable(aggregatorClient.listDataStores());
    }

    private void printDataStoreTable(ArrayList<DataStoreInfo> dataStoreInfos) throws ClassNotFoundException {
        logger.trace("dataStoreHealthReports");
        if(!quiet) {
            System.out.println("DataStoreCallable Count: " + dataStoreInfos.size());
            if(dataStoreInfos.size() > 0) {
                String tableHeader = String.format("| %-36s | %-36s | %-10s | %-10s | %-10s | %-25s | %-10s |", "AggregatorCallable ID", "DataStoreCallable ID", "Resource Count", "Used", "Total", "Class", "Uptime");
                System.out.println(tableHeader);
            }
        }
        for(DataStoreInfo dataStoreInfo : dataStoreInfos) {
            System.out.println("====== DataStore ======");
            System.out.println(dataStoreInfo.getId());
            System.out.println(dataStoreInfo.getPerformanceTier());
            System.out.println(dataStoreInfo.getResourceCount());
            System.out.println(dataStoreInfo.getUsedQuota());
            System.out.println(dataStoreInfo.getHardQuota());
            System.out.println("=======================");
        }

    }

    /*

    RESOURCES

     */

    private void listAndPrint(){
        ArrayList<Resource> resourceArrayList = aggregatorClient.listResources();
        printResourceTable(resourceArrayList);
    }

    private void printResourceTable(ArrayList<Resource> resourceArrayList){
        if(!quiet) {
            System.out.println("Resource Count: " + resourceArrayList.size());
            if(resourceArrayList.size() > 0) {
                String tableHeader = String.format("| %-36s | %-36s | %-10s | %-10s | %-20s |", "Resource ID", "Master Store ID", "Type", "Size", "Timestamp");
                System.out.println(tableHeader);
            }
        }
        for(Resource resource : resourceArrayList) {
            String uuid = resource.getID();
            String datastoreId = resource.getMetaString("master_store");
            String dataType = resource.getMetaString("data_type");
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
        System.out.println("UUID:      " + resource.getID());
        System.out.println("DataStoreCallable: " + resource.getMeta("datastoreId"));
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