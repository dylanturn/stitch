package stitch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import stitch.aggregator.AggregatorClient;
import stitch.datastore.DataStoreInfo;
import stitch.resource.Resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static spark.Spark.*;

public class StitchApi {

    static final Logger logger = Logger.getLogger(StitchApi.class);
    private static AggregatorClient aggregatorClient;

    public static void main(String[] args) throws Exception {

        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("stitch").setLevel(Level.TRACE);
        String aggregatorId = null;
        for(int i = 0; i < args.length; i++) {
            if(args[i].equals("--id")){
                aggregatorId = args[i+1];
                break;
            }
        }

        aggregatorClient = new AggregatorClient(aggregatorId);

        port(8080);

        get("/api/v1/health", (request, response) -> {
            response.type("application/json");
            return String.format("{\"transport_ready\": \"%s\", \"transport_connected\": \"%s\"}",
                    aggregatorClient.isRpcReady(),
                    aggregatorClient.isRpcConnected());

        });

        get("/api/v1/aggregator", (request, response) -> {
            response.type("application/json");
            return "[\"aggregator list goes here...\"]";

        });
        get("/api/v1/aggregator/:aggregator_id", (request, response) -> {
            response.type("application/json");
            return "{\"aggregator_details\": \"go here...\"}";
        });


        /* #### RESOURCES ### */

        post("/api/v1/resource", (request, response) -> {
            response.type("application/json");
            String requestBody = request.body();
            logger.info(requestBody);
            String resourceId = aggregatorClient.createResource(Resource.fromJson(requestBody));
            if(resourceId == null){
                response.status(500);
            } else {
                response.status(200);
            }
            return String.format("{ \"resource_id\": \"%s\" }", resourceId);

        });

        put("/api/v1/resource", (request, response) -> {
            response.type("application/json");
            boolean updateSuccess = aggregatorClient.updateResource(Resource.fromJson(request.body()));
            if(updateSuccess){
                response.status(204);
            } else {
                response.status(500);
            }
            return null;
        });

        get("/api/v1/resource", (request, response) -> {
            response.type("application/json");
            return resourceListToJson(aggregatorClient.listResources());

        });
        get("/api/v1/resource/:resource_id", (request, response) -> {
            response.type("application/json");
            return Resource.toJson(aggregatorClient.getResource(request.params(":resource_id")));
        });
        get("/api/v1/resource/:resource_id/data", (request, response) -> {
            response.type("application/json");
            return aggregatorClient.getResource(request.params(":resource_id")).getData();
        });
        get("/api/v1/resource/:resource_id/meta/:meta_key", (request, response) -> {
            response.type("application/json");
            return aggregatorClient.getResource(request.params(":resource_id")).getMeta(request.params(":meta_key"));
        });


        /* #### DATASTORES ### */
        get("/api/v1/datastore", (request, response) -> {
            response.type("application/json");
            return datastoreListToJson(aggregatorClient.listDataStores());
        });
        get("/api/v1/datastore/:datastore_id", (request, response) -> {
            response.type("application/json");
            return DataStoreInfo.toJson(aggregatorClient.getDatastore(request.params(":datastore_id")));
        });

    }

    private static String resourceListToJson(List<Resource> resourceList){
        Gson gson = new Gson();
        String rJson = gson.toJson(resourceList);
        return rJson;
        /*Collection<String> jsonList = new ArrayList<>();
        for(Resource resource : resourceList){
            jsonList.add(Resource.toJson(resource));
        }
        return gson.toJson(jsonList);*/
    }

    private static String datastoreListToJson(List<DataStoreInfo> datastoreInfoList){
        Gson gson = new Gson();
        String dsJson = gson.toJson(datastoreInfoList);
        logger.trace("getting output string: " + dsJson);
        return dsJson;
    }
}
