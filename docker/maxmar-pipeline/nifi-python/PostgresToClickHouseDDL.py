"""
PostgresToClickHouseDDL
=======================
Convert Postgres metadata to ClickHouse DDL.
Output: setiap DDL command jadi attribute terpisah dengan label.

Input attributes:
  - database_name (default: 'default')
  - table_name (default: 'unknown_table')
  - kafka.topic atau topic
  - kafka.brokers (default: 'kafka:9092')

Output attributes:
  - ddl.1.create_database
  - ddl.2.create_raw_table
  - ddl.3.create_kafka_table
  - ddl.4.create_materialized_view
  - ddl.5.create_final_table
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
class PostgresToClickHouseDDL(FlowFileTransform):
    
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "1.0.0"
        description = "Convert Postgres metadata to ClickHouse DDL - output per command as attributes"
        tags = ["postgres", "clickhouse", "ddl", "kafka"]

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
        'timestamp': 'DateTime',
        'timestamp without time zone': 'DateTime',
        'timestamp with time zone': 'DateTime',
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

    def transform(self, context, flowfile):
        try:
            attrs = dict(flowfile.getAttributes() or {})
            
            # Read attributes
            ch_db = attrs.get('database_name') or 'default'
            final_table = attrs.get('table_name') or 'unknown_table'
            kafka_topic = attrs.get('kafka.topic') or attrs.get('topic') or ''
            kafka_brokers = attrs.get('kafka.brokers') or 'kafka:9092'
            
            # Sanitize
            def sanitize(s):
                if not s: return s
                return re.sub(r'[^A-Za-z0-9]', '_', s)
            
            topic_sanitized = sanitize(kafka_topic or f"{ch_db}_{final_table}")
            raw_table = f"raw_{topic_sanitized}"
            kafka_table = f"kafka_{topic_sanitized}"
            mv_name = f"mv_{topic_sanitized}"
            
            # Parse JSON
            content = (flowfile.getContentsAsBytes() or b'').decode('utf-8') or '[]'
            try:
                data = json.loads(content)
            except:
                return FlowFileTransformResult(relationship="failure", contents=content, 
                    attributes={"error": "Invalid JSON"})
            
            # Parse columns
            final_cols = []
            has_id = False
            has_version = False
            
            if isinstance(data, list):
                for col in data:
                    col_name = col.get('column_name') or col.get('name')
                    raw_type = (col.get('data_type') or col.get('type') or '').lower()
                    if not col_name:
                        continue
                    
                    ch_type = self.TYPE_MAP.get(raw_type, 'String')
                    if str(col.get('is_nullable', '')).upper() == 'YES':
                        ch_type = f"Nullable({ch_type})"
                    
                    final_cols.append(f"    {col_name} {ch_type}")
                    
                    if col_name.lower() == 'id':
                        has_id = True
                    if col_name.lower() in ('_version', 'version', 'ts_ms'):
                        has_version = True
            else:
                for k, v in data.items():
                    final_cols.append(f"    {k} String")
                    if k.lower() == 'id':
                        has_id = True
                    if k.lower() in ('_version', 'version', 'ts_ms'):
                        has_version = True
            
            if not has_version:
                final_cols.append("    _version UInt64 DEFAULT 0")
            
            order_by = "ORDER BY id" if has_id else "ORDER BY tuple()"
            
            # === Generate DDL statements ===
            
            ddl_1_create_database = f"CREATE DATABASE IF NOT EXISTS {ch_db};"
            
            ddl_2_create_raw_table = f"""CREATE TABLE IF NOT EXISTS {ch_db}.{raw_table}
(
    kafka_topic String,
    kafka_partition UInt32 DEFAULT 0,
    kafka_offset UInt64 DEFAULT 0,
    payload String,
    received_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(received_at)
ORDER BY (kafka_topic, kafka_partition, kafka_offset);"""

            ddl_3_create_kafka_table = f"""CREATE TABLE IF NOT EXISTS {ch_db}.{kafka_table}
(
    payload String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = '{kafka_brokers}',
    kafka_topic_list = '{kafka_topic}',
    kafka_group_name = 'ch_{topic_sanitized}',
    kafka_format = 'JSONEachRow';"""

            ddl_4_create_materialized_view = f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {ch_db}.{mv_name}
TO {ch_db}.{raw_table}
AS
SELECT
    '{kafka_topic}' AS kafka_topic,
    payload AS payload,
    now() AS received_at
FROM {ch_db}.{kafka_table};"""

            ddl_5_create_final_table = f"""CREATE TABLE IF NOT EXISTS {ch_db}.{final_table}
(
{chr(10).join(final_cols)}
)
ENGINE = ReplacingMergeTree(_version)
{order_by};"""

            ddl_combined = "\n\n".join([
                ddl_1_create_database,
                ddl_2_create_raw_table,
                ddl_3_create_kafka_table,
                ddl_4_create_materialized_view,
                ddl_5_create_final_table
            ])
            
            # === Output attributes ===
            out_attrs = attrs.copy()
            
            # Metadata
            out_attrs['ch.database'] = ch_db
            out_attrs['ch.table.final'] = final_table
            out_attrs['ch.table.raw'] = raw_table
            out_attrs['ch.table.kafka'] = kafka_table
            out_attrs['ch.mv'] = mv_name
            
            # DDL per command dengan label
            out_attrs['ddl.1.create_database'] = ddl_1_create_database
            out_attrs['ddl.2.create_raw_table'] = ddl_2_create_raw_table
            out_attrs['ddl.3.create_kafka_table'] = ddl_3_create_kafka_table
            out_attrs['ddl.4.create_materialized_view'] = ddl_4_create_materialized_view
            out_attrs['ddl.5.create_final_table'] = ddl_5_create_final_table
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