package stitch;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import stitch.aggregator.AggregatorClient;
import stitch.util.Resource;

import java.util.ArrayList;
import java.util.concurrent.Callable;

import static stitch.ResourceProvider.logger;

@Command(name = "stitchcli", mixinStandardHelpOptions = true, version = "stitchcli 0.1",
        description = "Interacts with an aggregator via commandline")
class StitchCLI implements Callable<Integer> {

    private AggregatorClient aggregatorClient;

    @Parameters(index = "0", description = "The action to perform.")
    private String action;

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
        try {
            aggregatorClient = new AggregatorClient(aggregatorId);
            switch (action) {
                case "list":
                    listAndPrint();
                    break;

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

    private void listAndPrint(){
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
}