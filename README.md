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

### Object Configuration Discovery
Discovering datastores that are managed by an aggregator
```Java
public class ExampleConfigDiscovery {
    public static void main(String[] args) throws Exception {
        Map<String, String> filters = new HashMap<>();
        filters.put("type", ConfigItemType.DATASTORE.toString());
        filters.put("aggregator", endpointConfig.getConfigId());
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

public class ExampleAggregatorServer extends AggregatorServer {
    public ExampleAggregatorServer(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        super(endpointConfig);
    }
}
```
###### JSON Config
 ```json
{
    "uuid": "95a6495db3ef46c5aa98b428127c2cd4",
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
    "uuid": "95a6495db3ef46c5aa98b428127c2cd4",
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
    public HttpClient(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        super(endpointConfig);
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
    public HttpServer(ConfigItem endpointConfig) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        super(endpointConfig);
    }
}
```
###### JSON Config
 ```json
{
    "uuid": "95a6495db3ef46c5aa98b428127c2cd4",
    "name": "amqp",
    "type": "transport",
    "class": "stitch.transport.amqp",
    "client_class": Astitch.rpc.transport.amqp.AmqpClientch.rpc.transport.amqp.AMQPServer"
}
```
