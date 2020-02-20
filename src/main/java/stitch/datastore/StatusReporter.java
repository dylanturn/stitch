package stitch.datastore;

import stitch.aggregator.AggregatorClient;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Timer;
import java.util.TimerTask;

import static spark.Spark.get;

public class StatusReporter {

    private Timer timer;
    private TimerTask task;
    private long lastReportRun = -1;

    public StatusReporter(DataStoreServer dataStoreServer) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        this.timer = new Timer();
        this.task = new Reporter(dataStoreServer);

        // TODO: Add endpoing to stop or restart the datastore

        get("/api/v1/health", (request, response) -> {
            response.type("application/json");

            boolean datastoreAlive = dataStoreServer.isDataStoreAlive();
            boolean datastoreReady = dataStoreServer.isDataStoreReady();

            if(!datastoreAlive || !datastoreReady) {
                response.status(500);
            } else {
                response.status(200);
            }

            return String.format("{\"datastore_alive\": \"%s\", \"datastore_ready\": \"%s\"}",
                    datastoreAlive,
                    datastoreReady);
        });

        get("/api/v1/health/alive", (request, response) -> {
            response.type("application/json");

            boolean datastoreAlive = dataStoreServer.isDataStoreAlive();

            if(!datastoreAlive) {
                response.status(500);
            } else {
                response.status(200);
            }

            return String.format("{ \"datastore_alive\": \"%s\" }",
                    datastoreAlive);
        });

        get("/api/v1/health/ready", (request, response) -> {
            response.type("application/json");

            boolean datastoreReady = dataStoreServer.isDataStoreReady();

            if(!datastoreReady) {
                response.status(500);
            } else {
                response.status(200);
            }

            return String.format("{ \"datastore_ready\": \"%s\" }",
                    datastoreReady);
        });

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
            lastReportRun = Instant.now().toEpochMilli();
        }
    }

}
