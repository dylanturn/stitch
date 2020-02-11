
# File DataStore Server

### Overview
This module stores resources as files within an existing posix filesystem.

### Configuration
```json
{
"_id": {
        "$oid": "5e3cc21a1c9d440000aae873"
    },
  "uuid": " 15f24892d9cf4770a7e1f2e1cf0b47cb ",
  "name": "file_store_1",
  "type": "datastore",
  "class": "stitch.datastore.file.FileDataStoreServer",
  "transport": "95a6495db3ef46c5aa98b428127c2cd4",
  "aggregator": "7d1247d51e1545ed898727efd105efc2",
  "hard_quota_mb": 1024,
  "resource_count": 0,
  "store_path": "/Users/dylanturnbull/tmp/stitch_datastores"
}
```

```
{store_path}
    /{datastore_id}    
        /resources/
            {resource_id}


```

### TODO
1. Implement the ```initializeStore()`` method to make sure the DataStore directory structure exists.  