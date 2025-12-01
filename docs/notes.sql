
CREATE DATABASE kafka_cultivation;

CREATE TABLE kafka_cultivation.blocks (
  `message` String
) 
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:29092',
kafka_topic_list = 'pgserver.cultivation.blocks',
kafka_group_name = 'clickhouse_cultivation_blocks',
kafka_format = 'JSONAsString',
kafka_num_consumers = 1,
kafka_handle_error_mode = 'stream'

--------------------------------------------------------------------------------------------

CREATE DATABASE cultivation;

CREATE TABLE cultivation.blocks (
  `id` Int64,
  `site_id` Int64,
  `name` String,
  `user_id` Int64,
  `created_by` Int64,
  `created_at` DateTime64(6),
  `updated_by` Int64,
  `updated_at` DateTime64(6),
  `deleted_by` Nullable(Int64),
  `deleted_at` Nullable(DateTime64(6)),
  `op` String,
  `db` String,
  `schema` String,
  `table` String,
  `lsn` Int64,
  `ts_ms` Int64,
  `txId` Int64,
  `_kafka_key` String,
  `_kafka_topic` String,
  `_kafka_partition` Int64,
  `_kafka_offset` Int64,
  `_kafka_timestamp` DateTime,
  `_ingestion_time` DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(lsn) PARTITION BY toYYYYMM(created_at)
ORDER BY
  (id, lsn) SETTINGS index_granularity = 8192;
  
--------------------------------------------------------------------------------------------

CREATE DATABASE materialize_view_cultivation;

CREATE MATERIALIZED VIEW materialize_view_cultivation.blocks TO cultivation.blocks (
  `id` Int64,
  `site_id` Int64,
  `name` String,
  `user_id` Int64,
  `created_by` Int64,
  `created_at` Nullable(DateTime64(3)),
  `updated_by` Int64,
  `updated_at` Nullable(DateTime64(3)),
  `deleted_by` Nullable(Int64),
  `deleted_at` Nullable(DateTime64(3)),
  `op` String,
  `db` String,
  `schema` String,
  `table` String,
  `lsn` Int64,
  `ts_ms` Int64,
  `txId` Int64,
  `_kafka_key` String,
  `_kafka_topic` String,
  `_kafka_partition` Int64,
  `_kafka_offset` Int64,
  `_kafka_timestamp` DateTime
) AS
SELECT
  coalesce(
    JSONExtractInt(message, 'after', 'id'),
    JSONExtractInt(message, 'before', 'id')
  ) AS id,
  JSONExtractInt(message, 'after', 'site_id') AS site_id,
  JSONExtractString(message, 'after', 'name') AS name,
  JSONExtractInt(message, 'after', 'user_id') AS user_id,
  JSONExtractInt(message, 'after', 'created_by') AS created_by,
  if(
    JSONHas(message, 'after')
    AND JSONHas(JSONExtractRaw(message, 'after'), 'created_at'),
    parseDateTime64BestEffort(
      JSONExtractString(message, 'after', 'created_at')
    ),
    NULL
  ) AS created_at,
  JSONExtractInt(message, 'after', 'updated_by') AS updated_by,
  if(
    JSONHas(message, 'after')
    AND JSONHas(JSONExtractRaw(message, 'after'), 'updated_at'),
    parseDateTime64BestEffort(
      JSONExtractString(message, 'after', 'updated_at')
    ),
    NULL
  ) AS updated_at,
  if(
    JSONHas(message, 'after')
    AND JSONHas(JSONExtractRaw(message, 'after'), 'deleted_by'),
    JSONExtractInt(message, 'after', 'deleted_by'),
    NULL
  ) AS deleted_by,
  if(
    JSONHas(message, 'after')
    AND JSONHas(JSONExtractRaw(message, 'after'), 'deleted_at'),
    parseDateTime64BestEffort(
      JSONExtractString(message, 'after', 'deleted_at')
    ),
    NULL
  ) AS deleted_at,
  JSONExtractString(message, 'op') AS op,
  JSONExtractString(message, 'source', 'db') AS db,
  JSONExtractString(message, 'source', 'schema') AS schema,
  JSONExtractString(message, 'source', 'table') AS `table`,
  JSONExtractInt(message, 'source', 'lsn') AS lsn,
  JSONExtractInt(message, 'ts_ms') AS ts_ms,
  JSONExtractInt(message, 'source', 'txId') AS txId,
  _key AS _kafka_key,
  _topic AS _kafka_topic,
  _partition AS _kafka_partition,
  _offset AS _kafka_offset,
  _timestamp AS _kafka_timestamp
FROM
  kafka_cultivation.blocks
WHERE
  JSONHas(message, 'after')
  OR JSONHas(message, 'before');


