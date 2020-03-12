package stitch.datastore.resource;

import stitch.aggregator.metastore.DataStoreNotFoundException;
import stitch.util.HealthAlarm;
import stitch.util.configuration.item.ConfigItem;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ResourceManager implements ResourceStoreProvider {

    ConfigItem config;
    ResourceStoreProvider resourceStoreProvider;
    long hardQuota;

    public ResourceManager(ConfigItem config) throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        this.config = config;

        hardQuota = config.getConfigLong("hard_quota");

        Class<? extends ResourceStoreProvider> dataStoreCallableClass = config.getConfigClass("class");
        Constructor<?> dataStoreCallableClassConstructor = dataStoreCallableClass.getConstructor(ConfigItem.class);
        this.resourceStoreProvider = (ResourceStoreProvider) dataStoreCallableClassConstructor.newInstance(config);
    }

    @Override
    public String createResource(ResourceRequest resourceRequest) throws Exception {
        resourceRequest.setId(UUID.randomUUID().toString().replace("-", ""));
        return resourceStoreProvider.createResource(resourceRequest);
    }

    @Override
    public Resource getResource(String resourceId) throws Exception {
        return resourceStoreProvider.getResource(resourceId);
    }

    @Override
    public List<Resource> listResources() {
        return resourceStoreProvider.listResources();
    }

    @Override
    public List<Resource> findResources(String filter) {
        return resourceStoreProvider.findResources(filter);
    }

    @Override
    public boolean updateResource(ResourceRequest resourceRequest) throws Exception {
        return resourceStoreProvider.updateResource(resourceRequest);
    }

    @Override
    public boolean deleteResource(String resourceId) throws Exception {
        return resourceStoreProvider.deleteResource(resourceId);
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes) throws Exception {
        return resourceStoreProvider.writeData(resourceId, dataBytes);
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes, long offset) {
        return resourceStoreProvider.writeData(resourceId, dataBytes, offset);
    }

    @Override
    public byte[] readData(String resourceId) throws DataStoreNotFoundException {
        return resourceStoreProvider.readData(resourceId);
    }

    @Override
    public byte[] readData(String resourceId, long offset, long length) {
        return resourceStoreProvider.readData(resourceId, offset, length);
    }

    public String getPerformanceTier(){
        return config.getConfigString("performance_tier");
    }
    public long getHardQuota() { return hardQuota; }
    
    @Override
    public boolean isReady() {
        return this.resourceStoreProvider.isReady();
    }

    @Override
    public boolean isAlive() {
        // If the backend isn't ready then we'll just report the same thing.
        if(!resourceStoreProvider.isReady())
            return false;
        return true;
       // long lastReportTime = statusReporter.getLastReportRun();

        // Figure out how much time has passed since the last report.
        //long reportTimeDelta = Instant.now().toEpochMilli() - lastReportTime;

        // Make sure the status reporter is running by making sure the time since last run is less than two intervals.
      //  if(reportTimeDelta < (reportInterval*2)){
        //    return true;
      //  } else {
      //      return false;
       // }

    }

}

