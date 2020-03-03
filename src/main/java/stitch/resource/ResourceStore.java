package stitch.resource;

import java.util.List;
import java.util.Map;

public interface ResourceStore {
    String createResource(String performanceTier, long dataSize, String dataType, Map<String, Object> metaMap) throws Exception;
    String createResource(ResourceRequest resourceRequest) throws Exception;
    boolean updateResource(Resource resource) throws Exception;
    Resource getResource(String resourceId) throws Exception;
    boolean deleteResource(String resourceId) throws Exception;
    List<Resource> listResources();
    List<Resource> findResources(String filter);

    // Reads a sequence of bytes.
    byte[] readData(String resourceId);

    // Reads a sequence of bytes from this channel into the given buffers.
    // long read(ByteBuffer[] dsts);

    // Reads a sequence of bytes from this channel into a subsequence of the given buffers.
    // long read(ByteBuffer[] dsts, int offset, int length);

    // Reads a sequence of bytes from this channel into the given buffer, starting at the given file position.
    // int	read(ByteBuffer dst, long position);

}