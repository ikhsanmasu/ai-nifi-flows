"""
PostgresToClickHouseDebeziumDDL
================================
Convert Postgres metadata to ClickHouse DDL for Debezium CDC.
Menggunakan pola 3 database terpisah:
  - kafka_{db_name} untuk Kafka tables
  - {db_name} untuk target tables
  - materialize_view_{db_name} untuk materialized views

Input attributes:
  - database_name (nama database Postgres, e.g., 'cultivation')
  - table_name (nama table)
  - kafka.topic (Debezium topic, e.g., 'pgserver.cultivation.blocks')
  - kafka.brokers (default: 'kafka:29092')

Output attributes:
  - ddl.1.create_kafka_database
  - ddl.2.create_target_database
  - ddl.3.create_mv_database
  - ddl.4.create_kafka_table
  - ddl.5.create_target_table
  - ddl.6.create_materialized_view
  - ddl.combined (semua digabung)
"""

import json
import re
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.relationship import Relationship

try:
    from nifiapi.properties import PropertyDescriptor as PyPropertyDescriptor, StandardValidators
    _HAS_PY_PD = True
except Exception:
    PyPropertyDescriptor = None
    StandardValidators = None
    _HAS_PY_PD = False


# === BOILERPLATE ===
class JVMPropertyDescriptorWrapper:
    def __init__(self, prop_def):
        self.prop_def = prop_def
    def to_java_descriptor(self, gateway, controller_service_type_lookup):
        jvm = gateway.jvm
        PDBuilder = jvm.org.apache.nifi.components.PropertyDescriptor.Builder
        Validators = jvm.org.apache.nifi.processor.util.StandardValidators
        b = PDBuilder()
        try: b = b.name(self.prop_def["name"])
        except: pass
        try: b = b.displayName(self.prop_def["display"])
        except: pass
        try: b = b.description(self.prop_def["description"])
        except: pass
        try: b = b.defaultValue(self.prop_def["default"])
        except: pass
        try:
            v = self.prop_def.get("validator")
            if v == "NON_EMPTY": b = b.addValidator(Validators.NON_EMPTY_VALIDATOR)
        except: pass
        return b.build()

def _get_pds(defs):
    if _HAS_PY_PD:
        try:
            pds = set()
            for p in defs:
                try: builder = PyPropertyDescriptor.Builder()
                except: builder = None
                pd = None
                if builder:
                    try:
                        builder = builder.name(p["name"])
                        builder = builder.displayName(p["display"])
                        builder = builder.description(p["description"])
                        builder = builder.defaultValue(p["default"])
                        pd = builder.build()
                    except: pd = None
                pds.add(pd if pd else JVMPropertyDescriptorWrapper(p))
            return pds
        except: pass
    return {JVMPropertyDescriptorWrapper(p) for p in defs}


# === PROCESSOR ===
class PostgresToClickHouseDebeziumDDL(FlowFileTransform):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "2.0.0"
        description = "Generate ClickHouse DDL for Debezium CDC with 3-database pattern"
        tags = ["postgres", "clickhouse", "ddl", "kafka", "debezium", "cdc"]

    REL_SUCCESS = Relationship(name="success", description="DDL generated")
    REL_FAILURE = Relationship(name="failure", description="Failed")

    PROPERTY_DEFS = []

    TYPE_MAP = {
        'integer': 'Int32',
        'bigint': 'Int64',
        'smallint': 'Int16',
        'serial': 'Int32',
        'bigserial': 'Int64',
        'boolean': 'UInt8',
        'character varying': 'String',
        'varchar': 'String',
        'text': 'String',
        'char': 'String',
        'numeric': 'Decimal(18,4)',
        'decimal': 'Decimal(18,4)',
        'real': 'Float32',
        'double precision': 'Float64',
        'date': 'Date',
        'timestamp': 'DateTime64(6)',
        'timestamp without time zone': 'DateTime64(6)',
        'timestamp with time zone': 'DateTime64(6)',
        'json': 'String',
        'jsonb': 'String',
        'uuid': 'UUID',
    }

    def __init__(self, jvm=None, **kwargs):
        self.jvm = jvm
        super().__init__()

    def getRelationships(self):
        return {self.REL_SUCCESS, self.REL_FAILURE}

    def getPropertyDescriptors(self):
        return _get_pds(self.PROPERTY_DEFS)

    def _generate_json_extract(self, col_name, ch_type, is_nullable):
        """Generate JSONExtract expression for Debezium format"""
        base_type = ch_type.replace('Nullable(', '').replace(')', '')

        # Determine JSONExtract function based on type
        if base_type.startswith('Int') or base_type.startswith('UInt'):
            extract_func = 'JSONExtractInt'
        elif base_type.startswith('Float') or base_type.startswith('Decimal'):
            extract_func = 'JSONExtractFloat'
        elif base_type == 'UInt8':  # boolean
            extract_func = 'JSONExtractBool'
        elif base_type.startswith('DateTime'):
            # Special handling for datetime with empty string validation
            if is_nullable:
                return f"if(JSONHas(message, 'after') AND JSONHas(JSONExtractRaw(message, 'after'), '{col_name}') AND length(JSONExtractString(message, 'after', '{col_name}')) > 0, parseDateTime64BestEffort(JSONExtractString(message, 'after', '{col_name}')), NULL)"
            else:
                return f"if(length(JSONExtractString(message, 'after', '{col_name}')) > 0, parseDateTime64BestEffort(JSONExtractString(message, 'after', '{col_name}')), NULL)"
        else:
            extract_func = 'JSONExtractString'

        # Handle nullable columns
        if is_nullable:
            return f"if(JSONHas(message, 'after') AND JSONHas(JSONExtractRaw(message, 'after'), '{col_name}'), {extract_func}(message, 'after', '{col_name}'), NULL)"
        else:
            return f"{extract_func}(message, 'after', '{col_name}')"

    def transform(self, context, flowfile):
        try:
            attrs = dict(flowfile.getAttributes() or {})

            # Read attributes
            db_name = attrs.get('database_name') or 'default'
            table_name = attrs.get('table_name') or 'unknown_table'
            kafka_topic = attrs.get('kafka.topic') or attrs.get('topic') or ''
            kafka_brokers = attrs.get('kafka.brokers') or 'kafka:29092'

            # Database names based on pattern
            kafka_db = f"kafka_{db_name}"
            target_db = db_name
            mv_db = f"materialize_view_{db_name}"

            # Consumer group name
            consumer_group = f"clickhouse_{db_name}_{table_name}"

            # Parse JSON metadata
            content = (flowfile.getContentsAsBytes() or b'').decode('utf-8') or '[]'
            try:
                columns_data = json.loads(content)
            except:
                return FlowFileTransformResult(relationship="failure", contents=content,
                    attributes={"error": "Invalid JSON"})

            # Parse columns from Postgres metadata
            target_cols = []
            mv_select_cols = []
            has_id = False
            pk_column = None
            partition_column = None

            if isinstance(columns_data, list):
                for col in columns_data:
                    col_name = col.get('column_name') or col.get('name')
                    raw_type = (col.get('data_type') or col.get('type') or '').lower()
                    is_nullable = str(col.get('is_nullable', '')).upper() == 'YES'

                    if not col_name:
                        continue

                    ch_type = self.TYPE_MAP.get(raw_type, 'String')
                    if is_nullable:
                        ch_type = f"Nullable({ch_type})"

                    target_cols.append(f"    {col_name} {ch_type}")

                    # Generate materialized view SELECT expression
                    if col_name.lower() == 'id':
                        has_id = True
                        pk_column = col_name
                        # Use coalesce for id to handle DELETE operations
                        mv_select_cols.append(
                            f"    coalesce(JSONExtractInt(message, 'after', '{col_name}'), JSONExtractInt(message, 'before', '{col_name}')) AS {col_name}"
                        )
                    else:
                        extract_expr = self._generate_json_extract(col_name, ch_type, is_nullable)
                        mv_select_cols.append(f"    {extract_expr} AS {col_name}")

                    # Detect partition column (created_at, updated_at, etc.)
                    if col_name.lower() in ('created_at', 'updated_at', 'timestamp') and 'DateTime' in ch_type:
                        if not partition_column:  # Use first datetime column found
                            partition_column = col_name

            # Add Debezium metadata columns
            debezium_meta_cols = [
                "    op String",
                "    db String",
                "    schema String",
                "    `table` String",
                "    lsn Int64",
                "    ts_ms Int64",
                "    txId Int64"
            ]
            target_cols.extend(debezium_meta_cols)

            # Add Kafka metadata columns
            kafka_meta_cols = [
                "    _kafka_key String",
                "    _kafka_topic String",
                "    _kafka_partition Int64",
                "    _kafka_offset Int64",
                "    _kafka_timestamp DateTime",
                "    _ingestion_time DateTime DEFAULT now()"
            ]
            target_cols.extend(kafka_meta_cols)

            # Materialized view SELECT for Debezium metadata
            debezium_meta_select = [
                "    JSONExtractString(message, 'op') AS op",
                "    JSONExtractString(message, 'source', 'db') AS db",
                "    JSONExtractString(message, 'source', 'schema') AS schema",
                "    JSONExtractString(message, 'source', 'table') AS `table`",
                "    JSONExtractInt(message, 'source', 'lsn') AS lsn",
                "    JSONExtractInt(message, 'ts_ms') AS ts_ms",
                "    JSONExtractInt(message, 'source', 'txId') AS txId"
            ]
            mv_select_cols.extend(debezium_meta_select)

            # Materialized view SELECT for Kafka metadata
            kafka_meta_select = [
                "    _key AS _kafka_key",
                "    _topic AS _kafka_topic",
                "    _partition AS _kafka_partition",
                "    _offset AS _kafka_offset",
                "    _timestamp AS _kafka_timestamp"
            ]
            mv_select_cols.extend(kafka_meta_select)

            # Determine ORDER BY and PARTITION BY
            if has_id and pk_column:
                order_by = f"ORDER BY ({pk_column}, lsn)"
                engine = f"ReplacingMergeTree(lsn)"
            else:
                order_by = "ORDER BY (lsn)"
                engine = f"ReplacingMergeTree(lsn)"

            partition_by = f"PARTITION BY toYYYYMM({partition_column})" if partition_column else ""

            # === Generate DDL statements ===

            ddl_1_create_kafka_database = f"CREATE DATABASE IF NOT EXISTS {kafka_db};"

            ddl_2_create_target_database = f"CREATE DATABASE IF NOT EXISTS {target_db};"

            ddl_3_create_mv_database = f"CREATE DATABASE IF NOT EXISTS {mv_db};"

            ddl_4_create_kafka_table = f"""CREATE TABLE IF NOT EXISTS {kafka_db}.{table_name}
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{kafka_brokers}',
    kafka_topic_list = '{kafka_topic}',
    kafka_group_name = '{consumer_group}',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';"""

            # Join columns with commas
            target_cols_str = (',\n').join(target_cols)

            ddl_5_create_target_table = f"""CREATE TABLE IF NOT EXISTS {target_db}.{table_name}
(
{target_cols_str}
)
ENGINE = {engine}
{partition_by}
{order_by}
SETTINGS index_granularity = 8192;"""

            # Join SELECT columns with commas
            mv_select_str = (',\n').join(mv_select_cols)

            ddl_6_create_materialized_view = f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_db}.{table_name} TO {target_db}.{table_name}
AS
SELECT
{mv_select_str}
FROM {kafka_db}.{table_name}
WHERE JSONHas(message, 'after') OR JSONHas(message, 'before');"""

            ddl_combined = "\n\n".join([
                ddl_1_create_kafka_database,
                ddl_2_create_target_database,
                ddl_3_create_mv_database,
                ddl_4_create_kafka_table,
                ddl_5_create_target_table,
                ddl_6_create_materialized_view
            ])

            # === Output attributes ===
            out_attrs = attrs.copy()

            # Metadata
            out_attrs['ch.database.kafka'] = kafka_db
            out_attrs['ch.database.target'] = target_db
            out_attrs['ch.database.mv'] = mv_db
            out_attrs['ch.table.name'] = table_name
            out_attrs['ch.consumer.group'] = consumer_group

            # DDL per command dengan label
            out_attrs['ddl.1.create_kafka_database'] = ddl_1_create_kafka_database
            out_attrs['ddl.2.create_target_database'] = ddl_2_create_target_database
            out_attrs['ddl.3.create_mv_database'] = ddl_3_create_mv_database
            out_attrs['ddl.4.create_kafka_table'] = ddl_4_create_kafka_table
            out_attrs['ddl.5.create_target_table'] = ddl_5_create_target_table
            out_attrs['ddl.6.create_materialized_view'] = ddl_6_create_materialized_view
            out_attrs['ddl.combined'] = ddl_combined

            return FlowFileTransformResult(
                relationship="success",
                contents=ddl_combined,
                attributes=out_attrs
            )

        except Exception as e:
            try: self.logger.error(f"Transform error: {e}")
            except: pass
            return FlowFileTransformResult(
                relationship="failure",
                contents="",
                attributes={"error": str(e)}
            )
