package stitch.datastore;

import org.apache.log4j.Logger;
import stitch.amqp.AMQPClient;
import stitch.amqp.rpc.RPCPrefix;
import stitch.util.HealthReport;
import stitch.util.Resource;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;

public class DataStoreClient extends AMQPClient implements DataStore {

    static final Logger logger = Logger.getLogger(DataStoreClient.class);


    public DataStoreClient(String id) throws Exception {
        super(RPCPrefix.DATASTORE, id);
    }

    @Override
    public HealthReport reportHealth() throws Exception {
        return HealthReport.fromByteArray(call(getRouteKey(), "reportHealth", ""));
    }

    @Override
    public String createResource(Resource resource) throws Exception  {
        return new String(call(getRouteKey(), "createResource", resource), "UTF-8");
    }

    @Override
    public boolean updateResource(Resource resource) throws Exception {
        byte[] responseBytes = call(getRouteKey(), "updateResource", resource);
        if (responseBytes[0] == 0) { return false; }
        if (responseBytes[0] == 1) { return true; }
        return false;
    }

    @Override
    public Resource getResource(String resourceId) throws Exception {
        return Resource.fromByteArray(call(getRouteKey(), "getResource", resourceId));
    }

    @Override
    public boolean deleteResource(String resourceId) throws Exception {
        byte[] responseBytes = call(getRouteKey(), "deleteResource", resourceId);
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
            byte[] objectBytes = call(getRouteKey(), "listResources", "");
            ByteArrayInputStream bis = new ByteArrayInputStream(objectBytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            ArrayList<Resource> resourceArrayList = (ArrayList<Resource>)ois.readObject();
            return resourceArrayList;
        } catch(Exception error){
            logger.error(String.format("Failed to list the available resource metadata for datastore %s", getId()),error);
            return null;
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
