package stitch.resource;

import java.util.List;

public interface ResourceCallable {
    String createResource(Resource resource) throws Exception;
    boolean updateResource(Resource resource) throws Exception;
    Resource getResource(String resourceId) throws Exception;
    boolean deleteResource(String resourceId) throws Exception;
    List<Resource> listResources();
    List<Resource> findResources(String filter);
}