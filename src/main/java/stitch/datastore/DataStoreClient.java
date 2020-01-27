package stitch.datastore;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import stitch.rpc.RPCRequest;
import stitch.resource.Resource;
import stitch.rpc.metrics.RpcEndpointReport;
import stitch.rpc.transport.RpcCallableClient;
import stitch.rpc.transport.RpcTransportFactory;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.store.ConfigStore;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

public class DataStoreClient implements DataStore {

    static final Logger logger = Logger.getLogger(DataStoreClient.class);

    int timerInitialDelay = 5000;
    int timerReportPeriod = 5000;

    private Timer endpointReportTimer;
    protected ConfigItem endpointConfig;
    protected RpcCallableClient rpcClient;
    protected RpcEndpointReport peerEndpointReport;

    public DataStoreClient(String endpointId) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        //logger.setLevel(org.apache.log4j.Level.TRACE);
        endpointConfig = ConfigStore.loadConfigStore().getConfigItemById(endpointId);
        rpcClient = RpcTransportFactory.newRpcClient(endpointId);
        endpointReportTimer = new Timer();
        TimerTask task = new CheckHealth();
        endpointReportTimer.schedule(task, timerInitialDelay, timerReportPeriod);
    }

    public boolean isRpcReady(){
        return rpcClient.isReady();
    }
    public boolean isRpcConnected(){
        return rpcClient.isConnected();
    }

    private class CheckHealth extends TimerTask
    {
        public void run()
        {
            try {
                logger.trace("Requesting health report.");
                RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "getEndpointReport");
                peerEndpointReport = (RpcEndpointReport)rpcClient.invokeRPC(rpcRequest).getResponseObject();
                logger.trace("Received health report.");
                logger.trace(String.format("Node Health: %s", Boolean.toString(peerEndpointReport.getIsNodeHealthy())));
            } catch (Exception error) {
                logger.error("Failed to get heartbeat", error);
            }
        }
    }

    @Override
    public String createResource(Resource resource) throws Exception  {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "createResource")
                .putResourceArg(resource);
        return (String)rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public boolean updateResource(Resource resource) throws Exception {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "updateResource")
                .putResourceArg(resource);
        return (boolean)rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public Resource getResource(String resourceId) throws Exception {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "getResource")
                .putStringArg(resourceId);
        return (Resource) rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public boolean deleteResource(String resourceId) throws Exception {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "deleteResource")
                .putStringArg(resourceId);
        return (boolean)rpcClient.invokeRPC(rpcRequest).getResponseObject();
    }

    @Override
    public ArrayList<Resource> listResources() {
        return this.listResources(true);
    }

    @Override
    public ArrayList<Resource> listResources(boolean includeData) {
        RPCRequest rpcRequest = new RPCRequest("", rpcClient.getRpcAddress(), "listResources")
                .putBoolArg(includeData);
        try{
            return (ArrayList<Resource>)rpcClient.invokeRPC(rpcRequest).getResponseObject();

        } catch(Exception error){
            logger.error(String.format("Failed to list the available resource metadata for datastore %s", endpointConfig.getConfigName()), error);
            return null;
        }
    }

    // TODO: Implement some kind of resource search logic.
    @Override
    public ArrayList<Resource> findResources(String filter) {
        return listResources();
    }

    @Override
    public RpcEndpointReport getEndpointReport() throws IOException, InterruptedException, ClassNotFoundException {
        return peerEndpointReport;
    }
}
