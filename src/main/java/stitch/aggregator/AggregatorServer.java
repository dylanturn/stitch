package stitch.aggregator;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.LongString;
import org.apache.log4j.Logger;
import org.bson.Document;
import stitch.amqp.AMQPHandler;
import stitch.amqp.AMQPServer;
import stitch.amqp.rpc.RPCPrefix;
import stitch.datastore.DataStoreClient;
import stitch.util.Resource;
import stitch.util.ResponseBytes;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class AggregatorServer extends AMQPServer implements Aggregator, Runnable {

    static final Logger logger = Logger.getLogger(AggregatorServer.class);

    protected Document aggregatorArgs;
    protected HashMap<String, DataStoreClient> providerClients = new HashMap<>();

    public AggregatorServer(Document aggregatorArgs, Iterable<Document> providers) throws Exception {
        super(RPCPrefix.AGGREGATOR, aggregatorArgs.getString("uuid"));
        this.aggregatorArgs = aggregatorArgs;

        for(Document provider : providers){
            String providerUUID = provider.getString("uuid");
            providerClients.put(providerUUID, new DataStoreClient(providerUUID));
        }

        setHandler(new AMQPHandler(this) {
            @Override
            protected byte[] routeRPC(AMQP.BasicProperties messageProperties, byte[] messageBytes) {

                switch (messageProperties.getType()) {
                    case "RPC_listDataStores":
                        try{
                            return null;
                        } catch(Exception error){
                            logger.error("Failed to list datastores.", error);
                            return ResponseBytes.ERROR();
                        }
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

                        logger.info("Registering resource....");
                        Object callerPrefixObject = messageProperties.getHeaders().get("caller_prefix");

                        logger.info(String.format("Caller Prefix: %s", callerPrefixObject));
                        Object callerIdObject = messageProperties.getHeaders().get("caller_id");

                        try {
                            LongString longCallerPrefix = (LongString)callerPrefixObject;
                            LongString longCallerId = (LongString)callerPrefixObject;
                            String callerPrefix = longCallerPrefix.toString();
                            String callerId = longCallerId.toString();
                            Resource resource = Resource.fromByteArray(messageBytes);

                            logger.trace(String.format("Resource ID:   %s", resource.getUUID()));
                            logger.trace(String.format("Caller Prefix: %s", callerPrefix));
                            logger.trace(String.format("Caller ID:     %s", callerId));

                            registerResource(callerId, resource);

                        } catch (Exception error){
                            logger.error("Failed to register resource!", error);
                            logger.error(String.format("Caller Prefix: %s", callerPrefixObject));
                            logger.error(String.format("Caller ID:     %s", callerIdObject));
                            return ResponseBytes.ERROR();
                        }

                        return null;

                    default:
                        logger.error("Failed to match RPC method " + messageProperties.getType());
                        return ResponseBytes.ERROR();
                }

            }
        });
        new Thread(this).start();
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
        this.connect();
        this.registerResources();
    }

    public abstract void connect();
}
