package stitch.aggregator;

import org.apache.log4j.Logger;
import stitch.datastore.DataStoreClient;
import stitch.rpc.transport.*;
import stitch.rpc.transport.metrics.RpcEndpointReport;
import stitch.util.properties.StitchProperty;
import stitch.resource.Resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class AggregatorServer extends RpcAbstractServer implements Aggregator {

    static final Logger logger = Logger.getLogger(AggregatorServer.class);

    protected Iterable<StitchProperty> dataStoreProperties;

    protected RpcCallableServer rpcCallableServer;
    protected HashMap<String, DataStoreClient> providerClients = new HashMap<>();

    public AggregatorServer(StitchProperty rpcProperty, StitchProperty transportProperty, Iterable<StitchProperty> dataStoreProperties) throws InstantiationException, IllegalAccessException {
        super(rpcProperty, transportProperty);
        this.dataStoreProperties = dataStoreProperties;
        rpcCallableServer = RpcTransportFactory.newRpcServer(rpcProperty, transportProperty);
        for(StitchProperty datastoreProperty : dataStoreProperties){
            this.addDataStoreClient(datastoreProperty);
        }
    }

    public void addDataStoreClient(StitchProperty datastoreProperty) throws InstantiationException, IllegalAccessException {
        providerClients.put(datastoreProperty.getObjectId(), new DataStoreClient(datastoreProperty, transportProperty));
    }

    @Override
    public ArrayList<RpcEndpointReport> listDataStores() {
        ArrayList<RpcEndpointReport> dataStoreHealthReports = new ArrayList<>();
        for(DataStoreClient dataStoreClient : providerClients.values()){
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

        // Create a DataStore client instance for each datastore
        try {
            for (StitchProperty dataStoreProperty : dataStoreProperties) {
                providerClients.put(dataStoreProperty.getObjectId(), new DataStoreClient(dataStoreProperty, transportProperty));
            }
        } catch(IllegalAccessException error){
            logger.error("Failed to create datastore client!", error);
        } catch(InstantiationException error){
            logger.error("Failed to create datastore client!", error);
        }

        // Request a list of resources from all the DataStores and put them in the cache.
        this.registerResources();
    }

    public abstract void connect();

}



