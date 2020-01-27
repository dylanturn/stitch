# Stitch
[![CodeFactor](https://www.codefactor.io/repository/github/dylanturn/stitch/badge?s=13a678130938a23ae3e3f6d2b62050ea40634cd2)](https://www.codefactor.io/repository/github/dylanturn/stitch)

## Getting Started
```bash
# Starting the DataStore services
java -cp "target/*:target/libs/*" stitch.stitch.DataStoreMain
```

```bash
# Starting the Aggregator service
java -cp "target/*:target/libs/*" stitch.AggregatorMain
```

```bash
# List available datastores 
./stitchcli list datastores
```

```bash
# List available resources 
./stitchcli list resources
```

### Object Configuration
```Java
Map<String, String> filters = new HashMap<>();
filters.put("type", ConfigItemType.DATASTORE.toString());
filters.put("aggregator", endpointConfig.getConfigId());
for(ConfigItem dataStoreConfig : configStore.getConfigItemsByAttributes(filters)){
    dataStoreClients.put(dataStoreConfig.getConfigId(), new DataStoreClient(dataStoreConfig.getConfigId()));
}
```