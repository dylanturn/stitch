package stitch.datastore;


import org.apache.log4j.Logger;
import stitch.resource.ResourceCallable;
import stitch.transport.TransportCallableServer;
import stitch.rpc.RpcRequestHandler;
import stitch.transport.TransportFactory;
import stitch.util.configuration.item.ConfigItem;

import java.lang.reflect.InvocationTargetException;

public abstract class DataStoreServer implements DataStoreCallable, ResourceCallable, Runnable {

    static final Logger logger = Logger.getLogger(DataStoreServer.class);
    protected ConfigItem endpointConfig;
    protected TransportCallableServer rpcServer;

    protected long totalStorageSpace;
    protected long usedStorageSpace;
    protected long resourceCount;

    public DataStoreServer(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException {
        this.endpointConfig = endpointConfig;
        rpcServer = TransportFactory.newRpcServer(endpointConfig.getConfigId(), new RpcRequestHandler(this));
    }

    private void connectRpcTransport() throws IllegalAccessException, ClassNotFoundException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        rpcServer = TransportFactory.newRpcServer(endpointConfig.getConfigId(), new RpcRequestHandler(this));
        new Thread(rpcServer).start();
    }

    @Override
    public DataStoreReport getEndpointReport() {
        totalStorageSpace = endpointConfig.getConfigLong("totalStorageSpace");
        usedStorageSpace = getUsedStorage();
        resourceCount = getResourceCount();
        rpcServer.getRpcEndpointReporter().addExtra("totalStorageSpace", totalStorageSpace);
        rpcServer.getRpcEndpointReporter().addExtra("usedStorageSpace", usedStorageSpace);
        rpcServer.getRpcEndpointReporter().addExtra("resourceCount", resourceCount);
        return new DataStoreReport(endpointConfig, rpcServer.getRpcEndpointReporter());
    }

    public abstract void connectBackend();

    @Override
    public void run() {

        // Once the DataStoreServer has connected to the backend we can start the RPC transport.
        try {

            // Do whatever needs to be done to connect to the DataStoreCallable backend.
            this.connectBackend();

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