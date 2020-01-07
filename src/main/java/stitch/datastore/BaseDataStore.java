package stitch.datastore;

import com.rabbitmq.client.AMQP;
import org.apache.log4j.Logger;
import org.bson.Document;
import stitch.aggregator.Aggregator;
import stitch.aggregator.AggregatorClient;
import stitch.amqp.BaseAMQPHandler;
import stitch.util.BaseObject;
import stitch.amqp.BasicAMQPServer;
import stitch.util.Prefixes;
import stitch.util.Resource;
import stitch.util.ResponseBytes;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;

public abstract class BaseDataStore extends BaseObject implements DataStore, Runnable {

    static final Logger logger = Logger.getLogger(BaseDataStore.class);

    private BasicAMQPServer amqpServer;

    protected Document providerArgs;
    protected AggregatorClient aggregatorClient;
    protected String dsUUID;
    protected String dsType;
    protected String dsClass;
    protected String dsAggregator;

    public BaseDataStore(Document providerArgs) throws Exception {
        super(Prefixes.DATASTORE.toString(), providerArgs.getString("uuid"));

        this.providerArgs = providerArgs;
        dsUUID = providerArgs.getString("uuid");
        dsType = providerArgs.getString("type");
        dsClass = providerArgs.getString("class");
        dsAggregator = providerArgs.getString("aggregatorId");

        amqpServer = new BasicAMQPServer(getPrefix(), getId());
        amqpServer.setHandler(new BaseAMQPHandler(amqpServer) {
            @Override
            protected byte[] routeRPC(AMQP.BasicProperties messageProperties, byte[] messageBytes) {
                switch (messageProperties.getType()) {
                    case "RPC_createResource":
                        try {
                            Resource resource = Resource.fromByteArray(messageBytes);
                            logger.info("Creating new resource with UUID: " + resource.getUUID());
                            String resourceUUID = createResource(resource);
                            if(resourceUUID == null) {
                                return ResponseBytes.NULL();
                            } else {
                                return resourceUUID.getBytes();
                            }
                        } catch(Exception error){
                            logger.info("Faied to create resource!", error);
                            return ResponseBytes.ERROR();
                        }

                    case "RPC_updateResource":
                        try {
                            boolean updateResult = updateResource(Resource.fromByteArray(messageBytes));
                            byte[] booleanBytes = new byte[1];
                            booleanBytes[0] = (byte) (updateResult ? 1 : 0);
                            return booleanBytes;
                        } catch (Exception error){
                            logger.error("Failed to update resource!", error);
                            return ResponseBytes.ERROR();
                        }

                    case "RPC_getResource":
                        try {
                            Resource resource = getResource(bytesToString(messageBytes));
                            if(resource == null){
                                return ResponseBytes.NULL();
                            }
                            return Resource.toByteArray(resource);
                        } catch ( Exception error) {
                            logger.error(String.format("Failed to get Resource: %s",bytesToString(messageBytes)), error);
                            return ResponseBytes.ERROR();
                        }
                    case "RPC_deleteResource":
                        try {
                            Resource resource = getResource(bytesToString(messageBytes));
                            if(deleteResource(resource.getUUID())) {
                                return ResponseBytes.OK();
                            } else {
                                return ResponseBytes.ERROR();
                            }
                        } catch ( Exception error) {
                            logger.error(String.format("Failed to get Resource: %s",bytesToString(messageBytes)), error);
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
                        } catch(Exception error){
                            logger.error("Failed to list resources", error);
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
    public abstract void connect();
    @Override
    public void run() {
        this.connect();
        logger.info("Creating aggregator client for: " + dsAggregator);
        try {
            aggregatorClient = new AggregatorClient(dsAggregator);
            logger.info("Connected to aggregator, registering resources...");
            Iterable<Resource> resources = this.listResources(false);
            for (Resource resource : resources) {
                logger.info("Registering: " + resource.getUUID());
                aggregatorClient.registerResource(getId(), resource);
            }
        } catch(Exception error) {
            logger.error("Failed to connect to aggregator: " + dsAggregator, error);
        }
    }
    private String bytesToString(byte[] inBytes){
        try{
            String resourceId = new String(inBytes, "UTF-8");
            return resourceId;
        }catch(UnsupportedEncodingException error) {
            logger.error(String.format("Failed to get Resource due to unsupported encoding"),error);
            return null;
        }
    }
}
