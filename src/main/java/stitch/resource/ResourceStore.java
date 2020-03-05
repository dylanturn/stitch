package stitch.resource;

import stitch.aggregator.metastore.DataStoreNotFoundException;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

public interface ResourceStore {
    String createResource(String performanceTier, long dataSize, String dataType, Map<String, Object> metaMap) throws Exception;
    String createResource(ResourceRequest resourceRequest) throws Exception;

    boolean updateResource(Resource resource) throws Exception;
    boolean updateResource(String resourceId, ResourceRequest resourceRequest) throws Exception;

    Resource getResource(String resourceId) throws Exception;
    boolean deleteResource(String resourceId) throws Exception;
    List<Resource> listResources();
    List<Resource> findResources(String filter);

    // Reads a sequence of bytes.
    byte[] readData(String resourceId) throws DataStoreNotFoundException;
    byte[] readData(String resourceId, long offset, long length);

    // Writes a byte buffer
    int writeData(String resourceId, byte[] dataBytes) throws UnsupportedEncodingException, DataStoreNotFoundException;
    int writeData(String resourceId, byte[] dataBytes, long offset);
}