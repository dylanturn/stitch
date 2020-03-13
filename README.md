# Stitch
[![CodeFactor](https://www.codefactor.io/repository/github/dylanturn/stitch/badge?s=13a678130938a23ae3e3f6d2b62050ea40634cd2)](https://www.codefactor.io/repository/github/dylanturn/stitch)

## NOTES
2/5/2020 - (AMQP) The broadcast exchange had both datastores AND aggregators bound to it - I went ahead and modified the AMQP exchange setup. There are now two exchanges per endpoint type: ```stitch.aggregator.direct``` and ```stitch.aggregator.broadcast```.

## TODO:
1. Aggregator metastores need to keep track of the Endpoint records tha the datastores are sending in.
2. Maybe some way to track resource availability by the state of the host datastore?
3. Also need to think about eventually implementing the ability to have resource replicas.
4. Aggregators should probably allow you to create resource schemas.
5. Track resource metadata cardinality across a schema.


    Resource ID - The Id of the resource
    Epoch - A sequence number that identifies all copies that have the latest updates for a resource. The larger the number, the most up-to-date the copy of the resource keeping an older one from becoming a master.
    Mtime - A timestamp from the last time an update was made.
    Master- The datastore that contains the resources master (The contianer with the largest epoch number)
    ActiveServers - A list of available datastores that house the resource.
    InactiveServers - The id of datastores that host the container but are in maintenance mode.
    UnusedServers - The id of datastores from which no "heartbeat" has been received for quite some time.
    LogicalSizeMB - The logical size on disk of the resource
    
    
## Getting Started
```bash
# Starting the DataStore services
java -cp "target/*:target/libs/*" stitch.stitch.datastore.DataStoreMain
```

```bash
# Starting the Aggregator service
java -cp "target/*:target/libs/*" stitch.aggregator.AggregatorMain
```

```bash
# List available datastores 
./stitchcli list datastores
```

```bash
# List available resources 
./stitchcli list resources
```

### Object Configuration Discovery
Discovering datastores that are managed by an aggregator
```Java
public class ExampleConfigDiscovery {
    public static void main(String[] args) throws Exception {
        Map<String, String> filters = new HashMap<>();
        filters.put("type", ConfigItemType.DATASTORE.toString());
        filters.put("aggregator", config.getConfigId());
        for(ConfigItem dataStoreConfig : configStore.getConfigItemsByAttributes(filters)){
            dataStoreClients.put(dataStoreConfig.getConfigId(), new DataStoreClient(dataStoreConfig.getConfigId()));
        }
    }
}
```

## Implementing New Services

### Create a class that extends the services abstract class.

#### Aggregator
```java
import stitch.aggregator.AggregatorServer;
import stitch.util.configuration.item.ConfigItem;

public class ExampleAggregatorServer implements MetaStoreCallable {
    public ExampleAggregatorServer(ConfigItem config) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    }
}
```
###### JSON Config
 ```json
{
    "id": "95a6495db3ef46c5aa98b428127c2cd4",
    "name": "amqp",
    "type": "aggregator",
    "class": "com.foo.bar.ExampleAggregatorServer"
}
```
#### DataStore
```java
package com.foo.bar;
import stitch.datastore.DataStoreServer;
import stitch.util.configuration.item.ConfigItem;

import java.lang.reflect.InvocationTargetException;

public class ExampleDataStore extends DataStoreServer {
    public ExampleDataStore(ConfigItem configItem) throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException {
        super(configItem);
    }
}
```
###### JSON Config
 ```json
{
    "id": "95a6495db3ef46c5aa98b428127c2cd4",
    "name": "amqp",
    "type": "datastore",
    "class": "com.foo.bar.ExampleDataStore"
}
```
#### Transport
###### Transport Client
```java
package stitch.transport.amqp;

import stitch.transport.Transport;
import stitch.transport.TransportCallableClient;
import stitch.util.configuration.item.ConfigItem;

public class HttpClient extends RpcCallableAbstract implements RpcCallableClient {
    public HttpClient(ConfigItem config) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        super(config);
    }
}
```
###### Transport Server
```java
package stitch.transport.amqp;

import stitch.transport.Transport;
import stitch.transport.TransportCallableServer;
import stitch.util.configuration.item.ConfigItem;

public class HttpServer extends RpcCallableAbstract implements RpcCallableServer {
    public HttpServer(ConfigItem config) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        super(config);
    }
}
```
###### JSON Config
 ```json
{
    "id": "95a6495db3ef46c5aa98b428127c2cd4",
    "name": "amqp",
    "type": "transport",
    "class": "stitch.transport.amqp",
    "client_class": Astitch.rpc.transport.amqp.AmqpClientch.rpc.transport.amqp.AMQPServer"
}
```
