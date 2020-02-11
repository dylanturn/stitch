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
    String  replicaStatus
    Integer last_hash
    Long    last_seen
    
       