package stitch.datastore.resource;

import stitch.aggregator.metastore.DataStoreNotFoundException;

import java.util.List;

public class ResourceManager implements ResourceStoreProvider {

    public ResourceManager(){

    }

    @Override
    public boolean isReady() {
        return false;
    }

    @Override
    public boolean isAlive() {
        return false;
    }

    @Override
    public String createResource(ResourceRequest resourceRequest) throws Exception {
        return null;
    }

    @Override
    public Resource getResource(String resourceId) throws Exception {
        return null;
    }

    @Override
    public List<Resource> listResources() {
        return null;
    }

    @Override
    public List<Resource> findResources(String filter) {
        return null;
    }

    @Override
    public boolean updateResource(String resourceId, ResourceRequest resourceRequest) throws Exception {
        return false;
    }

    @Override
    public boolean deleteResource(String resourceId) throws Exception {
        return false;
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes) throws Exception {
        return 0;
    }

    @Override
    public int writeData(String resourceId, byte[] dataBytes, long offset) {
        return 0;
    }

    @Override
    public byte[] readData(String resourceId) throws DataStoreNotFoundException {
        return new byte[0];
    }

    @Override
    public byte[] readData(String resourceId, long offset, long length) {
        return new byte[0];
    }
}
