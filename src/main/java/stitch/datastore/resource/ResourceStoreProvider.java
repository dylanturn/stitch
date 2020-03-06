package stitch.datastore.resource;

import stitch.datastore.resource.ResourceStore;

public interface ResourceStoreProvider extends ResourceStore {
    boolean isReady();
    boolean isAlive();
}
