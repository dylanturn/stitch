package stitch.datastore;

import stitch.aggregator.AggregatorClient;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Timer;
import java.util.TimerTask;

public class StatusReporter {

    private Timer timer;
    private TimerTask task;

    public StatusReporter(DataStoreServer dataStoreServer) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        this.timer = new Timer();
        this.task = new Reporter(dataStoreServer);
    }

    public void schedule(int startDelay, int interval) {
        timer.schedule(task, startDelay, interval);
    }

    class Reporter extends TimerTask {

        private AggregatorClient aggregatorClient;
        private DataStoreServer dataStoreServer;

        public Reporter(DataStoreServer dataStoreServer) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
            this.dataStoreServer = dataStoreServer;
            this.aggregatorClient = new AggregatorClient(dataStoreServer.endpointConfig.getConfigString("aggregator"));
        }

        public void run()
        {
            DataStoreStatus status = new DataStoreStatus(dataStoreServer);
            try {
                aggregatorClient.reportDataStoreStatus(status);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
