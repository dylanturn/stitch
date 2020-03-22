### Select multiple fields
select meta_map.meta_key_1,created from datastore.performance_tier="general" where meta_map.meta_key_1="meta_val_1"

### Select with complex where
select * from * where created>1000 AND meta_map.meta_key_1=meta_val_1 AND (data_size = 5 OR data_size >= 7)

### Select with complex from
select * from datastore.performance_tier="general" where meta_map.meta_key_1="meta_val_1"