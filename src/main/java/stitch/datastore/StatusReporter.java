package stitch.datastore;

import org.apache.log4j.Logger;
import stitch.aggregator.AggregatorClient;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Timer;
import java.util.TimerTask;

public class StatusReporter {

    static final Logger logger = Logger.getLogger(StatusReporter.class);

    private Timer timer;
    private TimerTask task;
    private long lastReportRun = -1;

    public StatusReporter(DataStoreServer dataStoreServer) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        this.timer = new Timer();
        this.task = new Reporter(dataStoreServer);

    }

    public long getLastReportRun() { return lastReportRun; }

    public void schedule(int startDelay, int interval) {
        timer.schedule(task, startDelay, interval);
    }

    class Reporter extends TimerTask {

        private AggregatorClient aggregatorClient;
        private DataStoreServer dataStoreServer;

        public Reporter(DataStoreServer dataStoreServer) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
            this.dataStoreServer = dataStoreServer;
            aggregatorClient = new AggregatorClient(dataStoreServer.config.getConfigString("aggregator"));
        }

        public void run()
        {
            try {
                Thread.currentThread().setName(String.format("%s-status-reporter", dataStoreServer.getName()));
                DataStoreStatus status = new DataStoreStatus(dataStoreServer);
                aggregatorClient.reportDataStoreStatus(status);
                lastReportRun = Instant.now().toEpochMilli();
            } catch (IOException e) {
                logger.error("Caught IO Exception!", e);
            } catch (InterruptedException e) {
                logger.error("Caught Interrupted Exception!", e);
            }
        }
    }
}
