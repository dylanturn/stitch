package stitch.datastore;

import stitch.resource.ResourceStore;

public interface DataStore extends ResourceStore {
    boolean isReady();
    boolean isAlive();
}
