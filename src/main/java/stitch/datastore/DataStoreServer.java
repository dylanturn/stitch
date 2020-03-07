package stitch.datastore;


import org.apache.log4j.Logger;
import stitch.datastore.resource.ResourceManager;
import stitch.datastore.resource.ResourceStoreProvider;
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

    static final Logger logger = Logger.getLogger(DataStoreServer.class);

    protected ConfigItem config;
    private ResourceStoreProvider resourceStoreProvider;
    protected TransportCallableServer rpcServer;
    private StatusReporter statusReporter;
    private DataStoreAPI dataStoreAPI;

    private long startTime;
    protected long usedQuota;
    protected long hardQuota;
    private int reportInterval = 5000;
    private List<HealthAlarm> healthAlarmList = new ArrayList<>();

    public DataStoreServer(ConfigItem config) throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException {
        this.config = config;
        hardQuota = config.getConfigLong("hard_quota");
        usedQuota = config.getConfigLong("used_quota");
        startTime = Instant.now().toEpochMilli();
    }

    private void connectDataStoreBackend() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        resourceStoreProvider = new ResourceManager(config);
    }

    private void connectRpcTransport() throws IllegalAccessException, ClassNotFoundException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        rpcServer = TransportFactory.newRpcServer(config, new RpcRequestHandler(resourceStoreProvider));
        Thread rpcServerThread = new Thread(rpcServer);
        rpcServerThread.setName(String.format("%s-transport",config.getConfigName()));
        rpcServerThread.start();
    }

    private void startStatusReporter() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        statusReporter = new StatusReporter(this);
        statusReporter.schedule(reportInterval/2,reportInterval);
    }

    private void startDataStoreAPI() {
        dataStoreAPI = new DataStoreAPI(resourceStoreProvider);
    }

    public  String getName() {return config.getConfigName(); }
    public String getId(){
        return config.getConfigId();
    }
    public long getStartTime(){
        return startTime;
    }
    public String getPerformanceTier(){
        return config.getConfigString("performance_tier");
    }
    public long getUsedQuota() {
        return usedQuota;
    }
    public long getHardQuota() {
        return hardQuota;
    }
    public List<HealthAlarm> listAlarms(){ return healthAlarmList; }
    public ResourceStoreProvider getResourceStoreProvider() {
         return resourceStoreProvider;
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