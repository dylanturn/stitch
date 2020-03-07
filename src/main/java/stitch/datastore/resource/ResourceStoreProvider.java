package stitch.datastore.resource;

public interface ResourceStoreProvider extends ResourceStore {
    boolean isReady();
    boolean isAlive();
}
