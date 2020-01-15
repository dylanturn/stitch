package stitch.datastore;

import com.rabbitmq.client.AMQP;
import org.apache.log4j.Logger;
import org.bson.Document;
import stitch.amqp.AMQPHandler;
import stitch.amqp.rpc.RPCPrefix;
import stitch.util.*;
import stitch.amqp.AMQPServer;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import static stitch.util.Serializer.bytesToString;

public abstract class DataStoreServer extends AMQPServer implements DataStore {

    static final Logger logger = Logger.getLogger(DataStoreServer.class);

    protected Document providerArgs;
    protected String dsUUID;
    protected String dsType;
    protected String dsClass;

    public DataStoreServer(Document providerArgs) throws Exception {
        super(RPCPrefix.DATASTORE, providerArgs.getString("uuid"));

        this.providerArgs = providerArgs;
        dsUUID = providerArgs.getString("uuid");
        dsType = providerArgs.getString("type");
        dsClass = providerArgs.getString("class");

        setHandler(new AMQPHandler(this) {
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
                            logger.error(String.format("Failed to get Resource: %s", bytesToString(messageBytes)), error);
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
                    case "RPC_reportHealth":
                        try {
                            logger.trace("HeathReport requested");
                            return HealthReport.toByteArray(reportHealth());
                        } catch (Exception error){
                            logger.error("Failed to generage health report", error);
                            return null;
                        }
                    default:
                        logger.error("Failed to match RPC method " + messageProperties.getType());
                        return ResponseBytes.ERROR();
                }
            }
        });
        new Thread(this).start();
    }

    @Override
    public void run() {
        this.connect();
        this.consumeAMQP();
    }

    public abstract void connect();
}
