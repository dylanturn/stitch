package stitch.datastore.file;

import org.apache.log4j.Logger;

import stitch.datastore.DataStoreServer;
import stitch.resource.Resource;
import stitch.util.Serializer;
import stitch.util.configuration.item.ConfigItem;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileDataStoreServer extends DataStoreServer {

    static final Logger logger = Logger.getLogger(FileDataStoreServer.class);

    private long hardQuotaMb = -1;
    private long resourceCount = -1;
    private String storePath;
    private File storeFolder;
    private File resourceFolder;

    private String resourceIndexPath;
    private String resourceDataFolder;
    private MetaIndex metaIndex;

    public FileDataStoreServer(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IOException {
        super(endpointConfig);
        hardQuotaMb = endpointConfig.getConfigLong("hard_quota_mb");
        resourceCount = endpointConfig.getConfigLong("resource_count");
        storePath = endpointConfig.getConfigString("store_path");
        initializeStore();
    }

    private void createResourceIndex() throws IOException {
        // Initialize an empty hashmap that we can write out to .index
        metaIndex = new MetaIndex();
        Files.write(Paths.get(resourceIndexPath), Serializer.objectToBytes(metaIndex));

        // Clean up any dangling resources we might have.
        File file = new File(resourceDataFolder);
        file.delete();


    }

    private void initializeStore() throws IOException, ClassNotFoundException {

        // Construct the index and data store paths
        resourceIndexPath = String.format("%s/.index", storePath);
        resourceDataFolder = String.format("%s/resources", storePath);

        // Create or load the folder the houses the resource meta index
        storeFolder = new File(storePath);
        if(storeFolder.mkdirs()){
            // TODO: Log that we created the store folder.
        }

        // Load the resource index. If it doesn't exist we'll initialize a new one.
        File resourceIndexFile = new File(resourceIndexPath);
        if(!resourceIndexFile.exists())
            createResourceIndex();

        // Create or load the folder that houses the resources
        resourceFolder = new File(String.format("%s/resources", storePath));
        if(resourceFolder.mkdirs()){
            // TODO: Log that we created the resource folder.
        }

        Object indexObject = Serializer.bytesToObject(Files.readAllBytes(resourceIndexFile.toPath()));
        if (indexObject instanceof MetaIndex) {
            metaIndex = (MetaIndex)indexObject;
        }

    }

    @Override
    public String createResource(Resource resource) throws Exception {
        return null;
    }

    @Override
    public boolean updateResource(Resource resource) throws Exception {
        return false;
    }

    @Override
    public Resource getResource(String resourceId) throws Exception {
        MetaIndex.MetaIndexEntry metaIndexEntry = metaIndex.getIndexEntry(resourceId);
        Resource resource = metaIndexEntry.getResource();
        String dataPath = metaIndexEntry.getDataPath();
        File resourceDataFile = new File(dataPath);
        resource.setData(Files.readAllBytes(resourceDataFile.toPath()));
        return resource;
    }

    @Override
    public boolean deleteResource(String resourceId) throws Exception {
        return false;
    }

    @Override
    public List<Resource> listResources() {
        List<Resource> resourceList = new ArrayList<>();
        for(MetaIndex.MetaIndexEntry metaIndexEntry : metaIndex.listIndexEntries()){
            resourceList.add(metaIndexEntry.getResource());
        }
        return resourceList;
    }

    @Override
    public List<Resource> findResources(String filter) {
        return listResources();
    }

    @Override
    public boolean isDataStoreReady() {
        return false;
    }
}
