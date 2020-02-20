package stitch.datastore;


import org.apache.log4j.Logger;
import stitch.resource.ResourceCallable;
import stitch.transport.TransportCallableServer;
import stitch.rpc.RpcRequestHandler;
import stitch.transport.TransportFactory;
import stitch.util.configuration.item.ConfigItem;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;

public abstract class DataStoreServer implements DataStoreCallable, ResourceCallable, Runnable {

    static final Logger logger = Logger.getLogger(DataStoreServer.class);
    protected ConfigItem endpointConfig;
    protected TransportCallableServer rpcServer;
    private StatusReporter statusReporter;
    private long startTime;
    protected long usedQuota;
    protected long hardQuota;
    private DataStoreCallable callableDataStore;
    private int reportInterval = 5000;

    public DataStoreServer(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException {
        this.endpointConfig = endpointConfig;
        this.hardQuota = endpointConfig.getConfigLong("hard_quota");
        this.usedQuota = endpointConfig.getConfigLong("used_quota");
        this.startTime = Instant.now().toEpochMilli();
        statusReporter = new StatusReporter(this);
    }

    private void connectDataStoreBackend() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class<? extends DataStoreCallable> dataStoreCallableClass = endpointConfig.getConfigClass("class");
        Constructor<?> dataStoreCallableClassConstructor = dataStoreCallableClass.getConstructor(ConfigItem.class);
        this.callableDataStore = (DataStoreCallable) dataStoreCallableClassConstructor.newInstance(endpointConfig);
    }

    private void connectRpcTransport() throws IllegalAccessException, ClassNotFoundException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        rpcServer = TransportFactory.newRpcServer(endpointConfig, new RpcRequestHandler(callableDataStore));
        new Thread(rpcServer).start();
    }

    public String getId(){
        return this.endpointConfig.getConfigId();
    }
    public long getStartTime(){
        return this.startTime;
    }

    public String getPerformanceTier(){
        return endpointConfig.getConfigString("performance_tier");
    }

    public long getUsedQuota() {
        return usedQuota;
    }

    public long getHardQuota() {
        return hardQuota;
    }

    public TransportCallableServer getRpcServer(){
        return rpcServer;
    }

    @Override
    public boolean isDataStoreAlive() {

        // TODO: Need to implement better livelyness checks.

        long lastReportTime = statusReporter.getLastReportRun();

        // Figure out how much time has passed since the last report.
        long reportTimeDelta = Instant.now().toEpochMilli() - lastReportTime;

        // Make sure the status reporter is running by making sure the time since last run is less than two intervals.
        if(reportTimeDelta < (reportInterval*2)){
            return true;
        } else {
            return false;
        }

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

            logger.trace("Start the status reporter");
            this.statusReporter.schedule(reportInterval/2,reportInterval);

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