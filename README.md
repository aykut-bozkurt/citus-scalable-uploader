# citus-scalable-uploader
Converts citus distributed tables into csv format in scalable way such that each worker node runs its own conversions in parallel. After conversion you can implement and run your custom upload logic. Currently no upload method is implemented. You can add your own upload method inside the file `s3-uploader.sh`.

**Note**:
- Blocks any DDL and DML on the table's shards until the end of the conversions.
- Support only non-partitoned distributed tables.

**Prerequisites**
- You should have the file `s3-uploader.sh` on all worker nodes.
- You should create <output_folder> at the same path on all worker nodes.
- `scalable_copy_to_csv` should only be called on coordinator and only be called for distributed tables.

Example (Run on coordinator node)
```sql
-- create required functions for scalable conversion
\i scalable_copy.sql

-- create test table
create table dist(id int);
select create_distributed_table('dist','id');

-- insert random data into test table
insert into dist select s from generate_series(1,1000000) s;

-- do not use ~ in output folder
select scalable_copy_to_csv('dist','<output_folder>');
```
