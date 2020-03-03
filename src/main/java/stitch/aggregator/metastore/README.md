# Stitch MetaStore Schema

##### Resource MetaData Schema
    String  resource_id
    String  datastore_id
    Long    created
    String  data_type
    Long    data_size
    Long    epoch
    Long    mtime
    Integer last_hash
    Long    last_seen

##### DataStore MetaData Schema
    String  datastore_id
    String  performance_tier
    String  instance_class
    Long    used_quota
    Long    hard_quota
    Long    resource_count
    Integer last_hash
    Long    last_seen

##### Resource MetaData Schema
    String  resource_id
    String  datastore_id
    String  replicaRole
    Integer last_hash
    Long    last_seen
    
## Example Searches

FT.SEARCH datastore_meta "@datastore_id:333cf31f81784a8b93d2ae975de9a00a"
```java
class SearchFoo {
String searchQuery = String.format("@datastore_id:%s", datastoreId);
SearchResult searchResult = datastoreSchemaClient.search(new Query(searchQuery));
}
```

FT.AGGREGATE "*" LOAD 6 @datastore_id @hard_quota @used_quota @performance_tier @instance_class @resource_count

```java
class AggFoo {
AggregationBuilder aggregationBuilder = new AggregationBuilder();
aggregationBuilder
    .load("@datastore_id", "@hard_quota", "@used_quota", "@performance_tier", "@instance_class", "@resource_count")
    .filter(query);
}
```

FT.AGGREGATE datastore_meta "*"
LOAD 4 @datastore_id @hard_quota @used_quota @performance_tier
FILTER "@performance_tier=='general'"
APPLY "@hard_quota - @used_quota" AS available_quota
SORTBY 2 @available_quota DESC
LIMIT 0 1

```java
class AggFoo {
AggregationBuilder aggregationBuilder = new AggregationBuilder();
aggregationBuilder
    .load("@datastore_id", "@hard_quota", "@used_quota", "@performance_tier")
    .filter(query)
    .APPLY("@hard_quota - @used_quota", available_quota);
}
        
```