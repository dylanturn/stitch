package stitch.datastore;

import org.apache.log4j.Logger;
import org.bson.Document;
import stitch.amqp.AMQPHandler;
import stitch.amqp.AMQPPrefix;
import stitch.amqp.rpc.RPCRequest;
import stitch.amqp.rpc.RPCResponse;
import stitch.amqp.rpc.RPCStatusCode;
import stitch.resource.Resource;
import stitch.amqp.AMQPServer;

public abstract class DataStoreServer extends AMQPServer implements DataStore {

    static final Logger logger = Logger.getLogger(DataStoreServer.class);

    protected Document providerArgs;

    public DataStoreServer(Document providerArgs) throws Exception {
        super(AMQPPrefix.DATASTORE, providerArgs.getString("uuid"));

        this.providerArgs = providerArgs;
        this.addMetaData("type", providerArgs.getString("type"));
        this.addMetaData("class", providerArgs.getString("class"));

        setHandler(new DataStoreHandler(this));
        new Thread(this).start();
    }

    @Override
    public void run() {
        this.connect();
        this.consumeAMQP();
    }

    public abstract void connect();

    public String getStoreType(){
        return this.getMetaString("type");
    }

    public String getStoreClass(){
        return this.getMetaString("class");
    }
}
