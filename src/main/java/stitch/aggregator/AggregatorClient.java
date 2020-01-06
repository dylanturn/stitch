package stitch.aggregator;

import org.apache.log4j.Logger;
import stitch.amqp.BasicAMQPClient;
import stitch.util.BaseObject;
import stitch.util.Resource;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;

public class AggregatorClient extends BaseObject implements Aggregator {

    static final Logger logger = Logger.getLogger(AggregatorClient.class);
    private BasicAMQPClient amqpClient;

    public AggregatorClient(String id) throws Exception {
        super("stitch/aggregator", id);
        amqpClient = new BasicAMQPClient(this.getPrefix(), this.getId());
    }

    @Override
    public String createResource(Resource resource) {
        return null;
    }

    @Override
    public void updateResource(Resource resource) {
        try {
            amqpClient.call(getRouteKey(), "updateResource", Resource.toByteArray(resource));
        } catch(Exception error){
            logger.error("Failed to update the resource: " + resource.getUUID(), error);
        }
    }

    @Override
    public Resource getResource(String resourceId) {
        try {
            return Resource.fromByteArray(amqpClient.call(getRouteKey(), "getResource", resourceId));
        } catch(Exception error){}
        return null;
    }

    @Override
    public void deleteResource(String resourceId) {
        try {
            amqpClient.call(getRouteKey(), "deleteResource", resourceId.getBytes());
        } catch(Exception error){
            logger.error("Failed to delete resources!", error);
        }
    }

    @Override
    public ArrayList<Resource> findResources(String filter) {
        try {
            byte[] objectBytes = amqpClient.call(getRouteKey(), "findResources", filter);
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
            byte[] objectBytes = amqpClient.call(getRouteKey(), "listResources", "");
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
            amqpClient.call(getRouteKey(), "registerResource", resource);
        } catch(Exception error){
            logger.error("Failed to register resource!", error);
        }
    }
}
