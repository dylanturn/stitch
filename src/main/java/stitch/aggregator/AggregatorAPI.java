package stitch.aggregator;

import com.google.gson.Gson;
import stitch.aggregator.metastore.MetaStore;
import stitch.datastore.DataStoreInfo;
import stitch.resource.Resource;
import stitch.resource.ResourceRequest;

import java.util.List;

import static spark.Spark.*;

public class AggregatorAPI {

    private MetaStore metaStore;

    public AggregatorAPI(MetaStore metaStore) {
        this(metaStore, 8080);
    }

    public AggregatorAPI(MetaStore metaStore, int port){
        this.metaStore = metaStore;
        port(port);
        startAggregatorEndpoints();
        startDatastoreEndpoints();
        startResourceEndpoints();
    }

    /* #### AGGREGATOR ### */
    private void startAggregatorEndpoints(){
        get("/api/v1/aggregator", (request, response) -> {
            response.type("application/json");
            return "[\"aggregator stuff goes here...\"]";

        });

        get("/api/v1/aggregator/health", (request, response) -> {
            response.type("application/json");
            return "[\"aggregator health stuff goes here...\"]";

        });
    }

    /* #### DATASTORES ### */
    private void startDatastoreEndpoints(){
        get("/api/v1/datastore", (request, response) -> {
            response.type("application/json");
            String datastoreQuery = request.queryParams("query");
            DataStoreInfo[] datastoreInfoList;
            if(datastoreQuery == null)
                datastoreInfoList = metaStore.listDataStores();
            else
                datastoreInfoList = metaStore.findDataStores(datastoreQuery);

            Gson gson = new Gson();
            String dsJson = gson.toJson(datastoreInfoList);
            return dsJson;

        });

        get("/api/v1/datastore/:datastore_id", (request, response) -> {
            response.type("application/json");
            return DataStoreInfo.toJson(metaStore.getDatastore(request.params(":datastore_id")));
        });
    }

    /* #### RESOURCES ### */
    private void startResourceEndpoints(){
        post("/api/v1/resource", (request, response) -> {
            response.type("application/json");
            ResourceRequest resourceRequest = ResourceRequest.fromJson(request.body());
            System.out.println(ResourceRequest.toJson(resourceRequest));
            String resourceId = metaStore.createResource(resourceRequest);
            if(resourceId == null){
                response.status(500);
            } else {
                response.status(200);
            }
            return String.format("{ \"resource_id\": \"%s\" }", resourceId);

        });

        put("/api/v1/resource", (request, response) -> {
            response.type("application/json");
            boolean updateSuccess = metaStore.updateResource(Resource.fromJson(request.body()));
            if(updateSuccess){
                response.status(204);
            } else {
                response.status(500);
            }
            return null;
        });

        get("/api/v1/resource", (request, response) -> {
            response.type("application/json");
            return resourceListToJson(metaStore.listResources());

        });
        get("/api/v1/resource/:resource_id", (request, response) -> {
            response.type("application/json");
            return Resource.toJson(metaStore.getResource(request.params(":resource_id")));
        });
        get("/api/v1/resource/:resource_id/data", (request, response) -> {
            response.type("application/json");
            return metaStore.readData(request.params(":resource_id"));
        });
        get("/api/v1/resource/:resource_id/meta/:meta_key", (request, response) -> {
            response.type("application/json");
            return metaStore.getResource(request.params(":resource_id")).getMeta(request.params(":meta_key"));
        });
    }

    private static String resourceListToJson(List<Resource> resourceList){
        Gson gson = new Gson();
        String rJson = gson.toJson(resourceList);
        return rJson;
    }

    private static String datastoreListToJson(DataStoreInfo[] datastoreInfoList){
        Gson gson = new Gson();
        String dsJson = gson.toJson(datastoreInfoList);
        return dsJson;
    }

}
