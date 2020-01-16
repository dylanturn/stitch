package stitch.aggregator;

import org.apache.log4j.Logger;
import stitch.amqp.AMQPClient;
import stitch.amqp.rpc.RPCPrefix;
import stitch.amqp.HealthReport;
import stitch.util.Resource;
import stitch.util.Serializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

public class AggregatorClient extends AMQPClient implements Aggregator {

    static final Logger logger = Logger.getLogger(AggregatorClient.class);

    public AggregatorClient(String id) throws Exception {
        super(RPCPrefix.AGGREGATOR, id);
    }

    @Override
    public HealthReport reportHealth() {
        try {
            return HealthReport.fromByteArray(call(getRouteKey(), "requestHeartbeat", ""));
        } catch(InterruptedException error){
            logger.error("Interrupted while getting HealthReport from byte array?", error);
        } catch(IOException error){
            logger.error("IO Error while getting HealthReport from byte array", error);
        } catch (ClassNotFoundException error){
            logger.error("Failed to find the HealthReport class", error);
        }
        return null;
    }

    @Override
    public ArrayList<String> listDataStores() {
        try {
            return (ArrayList<String>)Serializer.bytesToObject(call(getRouteKey(), "listDataStores", ""));
        } catch(Exception error){
            logger.error("Failed to list resources!", error);
            return null;
        }
    }

    @Override
    public String createResource(Resource resource) {
        return null;
    }

    @Override
    public void updateResource(Resource resource) {
        try {
            call(getRouteKey(), "updateResource", Resource.toByteArray(resource));
        } catch(Exception error){
            logger.error("Failed to update the resource: " + resource.getUUID(), error);
        }
    }

    @Override
    public Resource getResource(String resourceId) {
        try {
            return Resource.fromByteArray(call(getRouteKey(), "getResource", resourceId));
        } catch(Exception error){}
        return null;
    }

    @Override
    public void deleteResource(String resourceId) {
        try {
            call(getRouteKey(), "deleteResource", resourceId.getBytes());
        } catch(Exception error){
            logger.error("Failed to delete resources!", error);
        }
    }

    @Override
    public ArrayList<Resource> findResources(String filter) {
        try {
            byte[] objectBytes = call(getRouteKey(), "findResources", filter);
            ByteArrayInputStream bis = new ByteArrayInputStream(objectBytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (ArrayList<Resource>)ois.readObject();
        } catch (Exception error) {
            logger.error("Failed to find resources!", error);
            return null;
        }

    }

    @Override
    public ArrayList<Resource> listResources() {
        try {
            byte[] objectBytes = call(getRouteKey(), "listResources", "");
            ByteArrayInputStream bis = new ByteArrayInputStream(objectBytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (ArrayList<Resource>)ois.readObject();
        } catch(Exception error){
            logger.error("Failed to list resources!", error);
            return null;
        }
    }

    @Override
    public void registerResource(String datastoreId, Resource resource) {
        try {
            logger.info(String.format("Registering resource: %s", resource.getUUID()));
            logger.info(String.format("Datastore Id:         %s", datastoreId));
            logger.info(String.format("Route Key:            %s", getRouteKey()));
            call(getRouteKey(), "registerResource", resource);
        } catch(Exception error){
            logger.error("Failed to register resource!", error);
        }
    }

    @Override
    public void run() {
        logger.info("Started Aggregator Client...");
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down aggregator client...");
    }
}
