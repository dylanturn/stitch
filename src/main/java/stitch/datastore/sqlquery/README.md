### Select multiple fields
SELECT meta_map.meta_key_1,created FROM datastore.performance_tier=general WHERE meta_map.meta_key_1=meta_val_1

### Select with complex where
SELECT * FROM * WHERE created>1000 AND (data_size = 5 OR data_size >= 7) AND meta_map.meta_key_1=meta_val_1

### Select with complex from
SELECT * FROM datastore.performance_tier=general WHERE meta_map.meta_key_1=meta_val_1