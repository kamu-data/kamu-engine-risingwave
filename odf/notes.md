# TODO
raw query support
remote format/encoding leftovers from source/sink
dump execution plan from `rw_relation_info.fragment`
see how to improve startup times
    disable leader election?
    disable PG frontend?
transition to RPC client - no postgres
test for long-term checkpoint size growth
run compactions and GC before shutdown

# Source
## Random
```sql
CREATE SOURCE rnd(id int, name varchar)
WITH (
    connector = 'datagen',
    fields.id.kind = 'sequence',
    fields.id.start = '1',
    fields.id.end = '10',
    fields.name.kind = 'random',
    fields.name.length = '10',
    datagen.rows.per.second = '1'
) FORMAT PLAIN ENCODE JSON;
```

## S3
```sql
CREATE TABLE s(
    id int,
    symbol varchar,
    price float
) 
WITH (
    connector = 's3_v2',
    s3.region_name = 'us-west-2',
    s3.bucket_name = 'X',
    s3.credentials.access = 'X',
    s3.credentials.secret = 'X',
    match_pattern = '*.ndjson'
) FORMAT PLAIN ENCODE JSON;
```

## Local FS
```sql
CREATE SOURCE s(
    id int,
    symbol varchar,
    price float
) 
WITH (
    connector = 'posix_fs',
    posix_fs.root = '/home/smikhtoniuk/Work/projects/opensource/risingwave/.priv/src',
    match_pattern = '*.csv'
) FORMAT PLAIN ENCODE CSV (
    without_header = 'true',
    delimiter = ',' 
);
```

# Sink
## Table
```sql
create table ss_t (symbol varchar primary key, price_avg float, price_max float, count bigint);
create sink ss into ss_t as select * from v;
```
