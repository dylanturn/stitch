package stitch.datastore.file;

import stitch.resource.Resource;

import java.io.Serializable;
import java.util.HashMap;


public class MetaIndex implements Serializable {

    private static final long serialVersionUID = 2489L;

    private HashMap<String, MetaIndexEntry> indexMap;

    public MetaIndex(){
        indexMap = new HashMap<>();
    }

    public void putIndexEntry(Resource resource, String dataPath){
        indexMap.put(resource.getID(), new MetaIndexEntry(resource, dataPath));
    }
    public MetaIndexEntry getIndexEntry(String resourceId){
        return indexMap.get(resourceId);
    }
    public MetaIndexEntry[] listIndexEntries(){
        return indexMap.values().toArray(new MetaIndexEntry[0]);
    }

    public class MetaIndexEntry {

        private Resource resource;
        private String dataPath;

        public MetaIndexEntry(Resource resource, String dataPath) {
            this.resource = resource;
            this.dataPath = dataPath;
        }

        public Resource getResource() {
            return resource;
        }

        public String getDataPath() {
            return dataPath;
        }
    }
}
