package stitch.datastore;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import stitch.datastore.resource.ResourceManager;
import stitch.transport.TransportCallableServer;
import stitch.rpc.RpcRequestHandler;
import stitch.transport.TransportFactory;
import stitch.util.HealthAlarm;
import stitch.util.configuration.item.ConfigItem;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class DataStoreServer implements Runnable {

    private static final Logger logger = LogManager.getLogger(DataStoreServer.class);

    ConfigItem config;
    ResourceManager resourceManager;
    TransportCallableServer rpcServer;

    StatusReporter statusReporter;
    DataStoreAPI dataStoreAPI;
    int reportInterval = 5000;
    long startTime;
    List<HealthAlarm> healthAlarmList = new ArrayList<>();

    public DataStoreServer(ConfigItem config) throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException {
        this.config = config;
        startTime = Instant.now().toEpochMilli();
    }

    public  String getName() {return config.getConfigName(); }
    public String getId() { return config.getConfigId(); }
    public long getStartTime() { return startTime; }
    public List<HealthAlarm> listAlarms(){ return healthAlarmList; }
    protected ConfigItem getConfig() { return config; }
    protected ResourceManager getResourceManager() { return resourceManager; }
    protected TransportCallableServer getRpcServer() { return rpcServer; }

    private void connectDataStoreBackend() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        resourceManager = new ResourceManager(config);
    }

    private void connectRpcTransport() throws IllegalAccessException, ClassNotFoundException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        rpcServer = TransportFactory.newRpcServer(config, new RpcRequestHandler(resourceManager));
        Thread rpcServerThread = new Thread(rpcServer);
        rpcServerThread.setName(String.format("%s-transport",config.getConfigName()));
        rpcServerThread.start();
    }

    private void startStatusReporter() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        statusReporter = new StatusReporter(this);
        statusReporter.schedule(reportInterval/2,reportInterval);
    }

    private void startDataStoreAPI() {
        dataStoreAPI = new DataStoreAPI(resourceManager);
    }

    @Override
    public void run() {

        // Once the DataStoreServer has connected to the backend we can start the RPC transport.
        try {

            // Do whatever needs to be done to connect to the ResourceStoreProvider backend.
            logger.trace("Connecting to the DataStores backend");
            connectDataStoreBackend();

            // Start up the RPC transport
            logger.trace("Connecting to the RPC Transport");
            connectRpcTransport();

            logger.trace("Start the status reporter");
            startStatusReporter();

            logger.trace("Start the DataStores API endpoint");
            startDataStoreAPI();

        } catch (IllegalAccessException e) {
            logger.error("Access not allowed!", e);
        } catch (ClassNotFoundException e) {
            logger.error("Failed to find class!", e);
        } catch (InstantiationException e) {
            logger.error("Failed to instantiate object!", e);
        } catch (InvocationTargetException e) {
            logger.error("Failed to invoke!", e);
        } catch (NoSuchMethodException e) {
            logger.error("No such method found!", e);
        }
    }
}