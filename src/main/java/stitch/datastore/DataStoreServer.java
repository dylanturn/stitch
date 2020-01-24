package stitch.datastore;

import org.bson.Document;
import stitch.rpc.transport.RpcAbstractServer;
import stitch.util.properties.StitchProperty;
import stitch.util.properties.StitchType;

public abstract class DataStoreServer extends RpcAbstractServer implements DataStore {

    public DataStoreServer(StitchProperty datastoreProperty, StitchProperty transportProperty) throws IllegalAccessException, InstantiationException {
        super(datastoreProperty, transportProperty);
    }

    @Override
    public void run() {
        this.connect();
        //this.consumeAMQP();
    }

    /*public String getStoreType(){
        return this.getMetaString("type");
    }

    public String getStoreClass(){
        return this.getMetaString("class");
    }*/

    public abstract void connect();
}
