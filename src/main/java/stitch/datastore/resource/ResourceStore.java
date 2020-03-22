package stitch.datastore.resource;

import stitch.aggregator.metastore.DataStoreNotFoundException;
import stitch.datastore.sqlquery.SearchQuery;

import java.util.List;

public interface ResourceStore {

    // RESOURCE CREATE
    String createResource(ResourceRequest resourceRequest) throws Exception;

    // RESOURCE READ/LIST/FIND
    Resource getResource(String resourceId) throws Exception;
    List<Resource> listResources();
    List<Resource> findResources(SearchQuery searchQuery) throws Exception;

    // RESOURCE UPDATE
    boolean updateResource(ResourceRequest resourceRequest) throws Exception;

    // RESOURCE DELETE
    boolean deleteResource(String resourceId) throws Exception;

    // RESOURCE DATA WRITE
    int writeData(String resourceId, byte[] dataBytes) throws Exception;
    int writeData(String resourceId, byte[] dataBytes, long offset);

    // RESOURCE DATA READ
    byte[] readData(String resourceId) throws DataStoreNotFoundException;
    byte[] readData(String resourceId, long offset, long length);
}