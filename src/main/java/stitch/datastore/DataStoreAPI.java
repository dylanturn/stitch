package stitch.datastore;

import stitch.datastore.resource.ResourceStoreProvider;

import static spark.Spark.get;

public class DataStoreAPI {

    public DataStoreAPI(ResourceStoreProvider resourceStoreProvider) {
        // TODO: Add endpoing to stop or restart the datastore

        get("/api/v1/health", (request, response) -> {
            response.type("application/json");

            boolean datastoreAlive = resourceStoreProvider.isAlive();
            boolean datastoreReady = resourceStoreProvider.isReady();

            if(!datastoreAlive || !datastoreReady) {
                response.status(500);
            } else {
                response.status(200);
            }

            return String.format("{\"datastore_alive\": \"%s\", \"datastore_ready\": \"%s\"}",
                    datastoreAlive,
                    datastoreReady);
        });

        get("/api/v1/health/alive", (request, response) -> {
            response.type("application/json");

            boolean datastoreAlive = resourceStoreProvider.isAlive();

            if(!datastoreAlive) {
                response.status(500);
            } else {
                response.status(200);
            }

            return String.format("{ \"datastore_alive\": \"%s\" }",
                    datastoreAlive);
        });

        get("/api/v1/health/ready", (request, response) -> {
            response.type("application/json");

            boolean datastoreReady = resourceStoreProvider.isReady();

            if(!datastoreReady) {
                response.status(500);
            } else {
                response.status(200);
            }

            return String.format("{ \"datastore_ready\": \"%s\" }",
                    datastoreReady);
        });
    }

}
