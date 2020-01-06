package stitch.datastore;

import org.apache.log4j.Logger;
import stitch.util.BaseObject;
import stitch.amqp.BasicAMQPClient;
import stitch.util.Resource;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;

public class DataStoreClient extends BaseObject implements DataStore {

    static final Logger logger = Logger.getLogger(DataStoreClient.class);
    private BasicAMQPClient amqpClient;

    public DataStoreClient(String id) throws Exception {
        super("stitch/datastore", id);
        amqpClient = new BasicAMQPClient(getPrefix(), getId());
    }

    // Returns the resources ID.
    @Override
    public String createResource(Resource resource) throws Exception  {
        return new String(amqpClient.call(getRouteKey(), "createResource", resource), "UTF-8");
    }

    @Override
    public boolean updateResource(Resource resource) throws Exception {
        byte[] responseBytes = amqpClient.call(getRouteKey(), "updateResource", resource);
        if (responseBytes[0] == 0) { return false; }
        if (responseBytes[0] == 1) { return true; }
        return false;
    }

    @Override
    public Resource getResource(String resourceId) throws Exception {
        return Resource.fromByteArray(amqpClient.call(getRouteKey(), "getResource", resourceId));
    }

    @Override
    public boolean deleteResource(String resourceId) throws Exception {
        byte[] responseBytes = amqpClient.call(getRouteKey(), "deleteResource", resourceId);
        if (responseBytes[0] == 0) { return false; }
        if (responseBytes[0] == 1) { return true; }
        return false;
    }

    @Override
    public ArrayList<Resource> listResources() {
        return this.listResources(true);
    }

    @Override
    public ArrayList<Resource> listResources(boolean includeData) {
        try{
            byte[] objectBytes = amqpClient.call(getRouteKey(), "listResources", "");
            ByteArrayInputStream bis = new ByteArrayInputStream(objectBytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            ArrayList<Resource> resourceArrayList = (ArrayList<Resource>)ois.readObject();
            return resourceArrayList;
        } catch(Exception error){
            logger.error(String.format("Failed to list the available resource metadata for stitch.datastore %s", getId()),error);
            return null;
        }
    }
}
