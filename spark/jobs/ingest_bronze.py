"""
Ingesta streaming Kafka/Redpanda -> Iceberg (capa bronze).

- Append-only e inmutable: bronze es la copia fiel de lo que publicó el core (evidencia de auditoría).
- Exactly-once hacia Iceberg: offsets en el checkpoint + commit atómico de snapshot por micro-batch.
  Los duplicados que vengan del outbox (at-least-once) se conservan aquí y se eliminan en silver.
- El payload se guarda crudo (string JSON): un cambio de esquema en el origen nunca rompe la ingesta.

Portable a AWS Glue Streaming: mismo código, cambiando solo la configuración del catálogo.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "audit.events")
TABLE = os.getenv("BRONZE_TABLE", "lakehouse.bronze.audit_events")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/opt/checkpoints/bronze_audit_events")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "30 seconds")

BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    event_id        STRING    COMMENT 'Id único del evento (clave de deduplicación)',
    aggregate_type  STRING,
    aggregate_id    STRING,
    event_type      STRING,
    schema_version  INT       COMMENT 'Versión del contrato del payload',
    occurred_at     TIMESTAMP COMMENT 'Momento del hecho de negocio (UTC)',
    payload         STRING    COMMENT 'JSON crudo tal como lo emitió el core',
    raw_value       STRING    COMMENT 'Mensaje original solo si no se pudo interpretar (cuarentena)',
    kafka_partition INT,
    kafka_offset    BIGINT,
    ingested_at     TIMESTAMP COMMENT 'Momento de llegada al lakehouse (UTC)'
)
USING iceberg
PARTITIONED BY (days(occurred_at))
TBLPROPERTIES (
    'format-version'                          = '2',
    'write.parquet.compression-codec'         = 'zstd',
    'write.target-file-size-bytes'            = '134217728',
    'write.distribution-mode'                 = 'hash',
    'write.delete.mode'                       = 'copy-on-write',
    'write.update.mode'                       = 'copy-on-write',
    'write.merge.mode'                        = 'copy-on-write',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'    = '50',
    'history.expire.max-snapshot-age-ms'      = '604800000',
    'history.expire.min-snapshots-to-keep'    = '10'
)
"""

ENVELOPE = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("aggregate_type", T.StringType()),
    T.StructField("aggregate_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("occurred_at", T.StringType()),
    T.StructField("payload", T.StringType()),
])


def main() -> None:
    spark = SparkSession.builder.appName("ingest-bronze-audit").getOrCreate()

    namespace = TABLE.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(BRONZE_DDL)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "false")
        .load()
    )

    value = F.col("value").cast("string")
    parsed = raw.select(
        F.from_json(value, ENVELOPE).alias("e"),
        value.alias("value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )

    bronze = parsed.select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.aggregate_type").alias("aggregate_type"),
        F.col("e.aggregate_id").alias("aggregate_id"),
        F.col("e.event_type").alias("event_type"),
        F.col("e.schema_version").alias("schema_version"),
        F.col("e.occurred_at").cast("timestamp").alias("occurred_at"),
        F.col("e.payload").alias("payload"),
        F.when(F.col("e.event_id").isNull(), F.col("value")).alias("raw_value"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.current_timestamp().alias("ingested_at"),
    )

    query = (
        bronze.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=TRIGGER)
        .option("checkpointLocation", CHECKPOINT)
        .option("fanout-enabled", "true")
        .toTable(TABLE)
    )

    print(f"[ingest] {TOPIC} -> {TABLE} cada {TRIGGER}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
