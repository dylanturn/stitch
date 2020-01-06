package stitch.aggregator;

import com.rabbitmq.client.AMQP;
import org.apache.log4j.Logger;
import org.bson.Document;
import stitch.amqp.BaseAMQPHandler;
import stitch.amqp.BasicAMQPServer;
import stitch.datastore.BaseDataStore;
import stitch.datastore.DataStoreClient;
import stitch.util.BaseObject;
import stitch.util.Prefixes;
import stitch.util.Resource;
import stitch.util.ResponseBytes;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseAggregator extends BaseObject implements Aggregator, Runnable {

    static final Logger logger = Logger.getLogger(BaseDataStore.class);

    protected Document aggregatorArgs;
    protected HashMap<String, DataStoreClient> providerClients = new HashMap<>();
    private BasicAMQPServer amqpServer;

    public BaseAggregator(Document aggregatorArgs, Iterable<Document> providers) throws Exception {
        super("stitch/aggregator", aggregatorArgs.getString("uuid"));
        this.aggregatorArgs = aggregatorArgs;

        for(Document provider : providers){
            String providerUUID = provider.getString("uuid");
            providerClients.put(providerUUID, new DataStoreClient(providerUUID));
        }

        amqpServer = new BasicAMQPServer(this.getPrefix(), this.getId());
        amqpServer.setHandler(new BaseAMQPHandler(amqpServer) {
            @Override
            protected byte[] routeRPC(AMQP.BasicProperties messageProperties, byte[] messageBytes) {

                switch (messageProperties.getType()) {
                    case "RPC_createResource":
                        try {
                            Resource resource = Resource.fromByteArray(messageBytes);
                            String resourceId = createResource(resource);
                            return resourceId.getBytes();
                        } catch (Exception error){
                            logger.error("Failed to create resource!", error);
                            return ResponseBytes.ERROR();
                        }
                    case "RPC_updateResource":
                        try {
                            Resource resource = Resource.fromByteArray(messageBytes);
                            updateResource(resource);
                        } catch (Exception error){
                            logger.error("Failed to update resource!", error);
                            return ResponseBytes.ERROR();
                        }
                    case "RPC_getResource":
                        try {
                            String resourceId = new String(messageBytes, "UTF-8");
                            return Resource.toByteArray(getResource(resourceId));
                        } catch (Exception error){
                            logger.error("Failed to get resource!", error);
                            return ResponseBytes.ERROR();
                        }
                    case "RPC_deleteResource":
                        try {
                            String resourceId = new String(messageBytes, "UTF-8");
                            deleteResource(resourceId);
                        } catch (Exception error){
                            logger.error("Failed to delete resource!", error);
                            return ResponseBytes.ERROR();
                        }
                    case "RPC_findResources":
                        try {
                            String filterString = new String(messageBytes, "UTF-8");
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(findResources(filterString));
                            oos.flush();
                            byte[] resourceList= bos.toByteArray();
                            if(resourceList == null){
                                return ResponseBytes.NULL();
                            } else {
                                return resourceList;
                            }
                        } catch (Exception error){
                            logger.error("Failed to find resource!", error);
                            return ResponseBytes.ERROR();
                        }
                    case "RPC_listResources":
                        try {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(listResources());
                            oos.flush();
                            byte[] resourceList= bos.toByteArray();
                            if(resourceList == null){
                                return ResponseBytes.NULL();
                            } else {
                                return resourceList;
                            }
                        } catch (Exception error){
                            logger.error("Failed to list resources!", error);
                            return ResponseBytes.ERROR();
                        }
                    case "RPC_registerResource":
                        try {
                            String callerPrefix = (String)messageProperties.getHeaders().get("caller_prefix");
                            String callerId = (String)messageProperties.getHeaders().get("caller_id");
                            if(callerPrefix == Prefixes.DATASTORE.toString()) {
                                registerResource(callerId, Resource.fromByteArray(messageBytes));
                            } else {
                                logger.info(String.format("WARNING: Register resource called by %s with prefix %s", callerId, callerPrefix));
                            }
                        } catch (Exception error){
                            logger.error("Failed to register resource!", error);
                            return ResponseBytes.ERROR();
                        }
                    default:
                        logger.error("Failed to match RPC method " + messageProperties.getType());
                        return ResponseBytes.ERROR();
                }

            }
        });
        new Thread(amqpServer).start();
    }

    @Override
    public void run() {
        this.connect();
        this.registerResources();
    }

    public abstract void connect();

    private void registerResources(){
        for(Map.Entry<String, DataStoreClient> dataStoreClient : providerClients.entrySet()){
            ArrayList<Resource> resourceArray = dataStoreClient.getValue().listResources();
            for(Resource resource : resourceArray) {
                this.registerResource(dataStoreClient.getKey(), resource);
            }
        }
    }
}
