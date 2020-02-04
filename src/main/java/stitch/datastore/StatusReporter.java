package stitch.datastore;

import java.util.Timer;
import java.util.TimerTask;

public class StatusReporter {

    private Timer timer;
    private TimerTask task;
    private DataStoreServer dataStoreServer;

    public StatusReporter(DataStoreServer dataStoreServer) {
        this.dataStoreServer = dataStoreServer;
        this.timer = new Timer();
        this.task = new Reporter(dataStoreServer);
    }

    public void schedule(int startDelay, int interval) {
        timer.schedule(task, startDelay, interval);
    }

    class Reporter extends TimerTask {

        private DataStoreServer dataStoreServer;

        public Reporter(DataStoreServer dataStoreServer){
            this.dataStoreServer = dataStoreServer;
        }



        public void run()
        {
            System.out.println("Do stuff for: " + this.dataStoreServer.getId());
        }
    }

}
