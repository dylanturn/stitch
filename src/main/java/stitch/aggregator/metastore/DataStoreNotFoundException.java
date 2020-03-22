package stitch.aggregator.metastore;

public class DataStoreNotFoundException extends Exception {
    public DataStoreNotFoundException(String storeId) {
        super(String.format("Failed to find resource store %s", storeId));
    }
    public DataStoreNotFoundException(String storeId, String errorMessage) {
        super(String.format("Failed to find resource store %s\nError Message: %s", storeId, errorMessage));
    }
}