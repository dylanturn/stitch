package stitch.aggregator;

import org.apache.log4j.Logger;
import org.bson.Document;
import stitch.amqp.AMQPServer;
import stitch.amqp.HealthReport;
import stitch.amqp.AMQPPrefix;
import stitch.datastore.DataStoreClient;
import stitch.resource.Resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class AggregatorServer extends AMQPServer implements Aggregator, Runnable {

    static final Logger logger = Logger.getLogger(AggregatorServer.class);

    protected Document aggregatorArgs;
    protected HashMap<String, DataStoreClient> providerClients = new HashMap<>();

    public AggregatorServer(Document aggregatorArgs, Iterable<Document> providers) throws Exception {
        super(AMQPPrefix.AGGREGATOR, aggregatorArgs.getString("uuid"));
        this.aggregatorArgs = aggregatorArgs;

        for(Document provider : providers){
            String providerUUID = provider.getString("uuid");
            providerClients.put(providerUUID, new DataStoreClient(providerUUID));
        }

        setHandler(new AggregatorHandler(this));
        new Thread(this).start();
    }

    @Override
    public ArrayList<HealthReport> listDataStores() {
        ArrayList<HealthReport> dataStoreHealthReports = new ArrayList<>();
        for(DataStoreClient dataStoreClient : providerClients.values()){
            dataStoreHealthReports.add(dataStoreClient.getLastHealthReport());
        }
        return dataStoreHealthReports;
    }

    private void registerResources(){
        logger.info("Registering datastore resources!");
        for(Map.Entry<String, DataStoreClient> dataStoreClient : providerClients.entrySet()){
            logger.info("Registering datastore resources.");
            ArrayList<Resource> resourceArray = dataStoreClient.getValue().listResources();
            for(Resource resource : resourceArray) {
                this.registerResource(dataStoreClient.getKey(), resource);
            }
        }
    }

    @Override
    public void run() {
        // Make sure the Aggregator has connected to the cache
        this.connect();
        // Begin consuming from the Aggregators' queue
        this.consumeAMQP();
        // Request a list of resources from all the DataStores and put them in the cache.
        this.registerResources();
    }

    public abstract void connect();
}
