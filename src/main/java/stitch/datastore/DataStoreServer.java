package stitch.datastore;


import org.apache.log4j.Logger;
import stitch.aggregator.AggregatorServer;
import stitch.resource.ResourceCallable;
import stitch.transport.TransportCallableServer;
import stitch.rpc.RpcRequestHandler;
import stitch.transport.TransportFactory;
import stitch.util.configuration.item.ConfigItem;
import sun.security.krb5.Config;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;

public class DataStoreServer implements Runnable {

    static final Logger logger = Logger.getLogger(DataStoreServer.class);
    protected ConfigItem endpointConfig;
    protected TransportCallableServer rpcServer;
    private StatusReporter statusReporter;
    private long startTime;
    private DataStoreCallable callableDataStore;

    public DataStoreServer(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException {
        this.endpointConfig = endpointConfig;
        this.startTime = Instant.now().toEpochMilli();
        statusReporter = new StatusReporter(this);
    }

    private void connectDataStoreBackend() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class<? extends DataStoreCallable> dataStoreCallableClass = endpointConfig.getConfigClass("class");
        Constructor<?> dataStoreCallableClassConstructor = dataStoreCallableClass.getConstructor(ConfigItem.class);
        this.callableDataStore = (DataStoreCallable) dataStoreCallableClassConstructor.newInstance(endpointConfig);
    }

    private void connectRpcTransport() throws IllegalAccessException, ClassNotFoundException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        rpcServer = TransportFactory.newRpcServer(endpointConfig.getConfigId(), new RpcRequestHandler(callableDataStore));
        new Thread(rpcServer).start();
    }

    public String getId(){
        return this.endpointConfig.getConfigId();
    }
    public long getStartTime(){
        return this.startTime;
    }
    public TransportCallableServer getRpcServer(){
        return rpcServer;
    }

    @Override
    public void run() {

        // Once the DataStoreServer has connected to the backend we can start the RPC transport.
        try {

            // Do whatever needs to be done to connect to the DataStoreCallable backend.
            logger.trace("Connecting to the DataStores backend");
            connectDataStoreBackend();

            // Start up the RPC transport
            logger.trace("Connecting to the RPC Transport");
            connectRpcTransport();

        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }
}