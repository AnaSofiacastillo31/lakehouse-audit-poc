"""
Mantenimiento de tablas Iceberg (equivalente local a un Glue Job programado o a los
"table optimizers" del AWS Glue Data Catalog).

1. rewrite_data_files  -> compacta los archivos pequeños que genera el streaming (small files problem).
2. rewrite_manifests   -> reagrupa manifests para que la planificación de consultas siga siendo rápida.
3. expire_snapshots    -> limita el time-travel y libera archivos que ya no referencia ningún snapshot.
4. remove_orphan_files -> borra basura de escrituras fallidas (con margen de seguridad).

Todo corre con concurrencia optimista: la ingesta streaming puede seguir escribiendo mientras se compacta
(un append no entra en conflicto con un rewrite de archivos existentes).
"""
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

CATALOG = "lakehouse"
TABLES = os.getenv("MAINTENANCE_TABLES", "bronze.audit_events,silver.audit_events").split(",")
SNAPSHOT_RETENTION_MINUTES = int(os.getenv("SNAPSHOT_RETENTION_MINUTES", "60"))
ORPHAN_RETENTION_HOURS = int(os.getenv("ORPHAN_RETENTION_HOURS", "24"))
RETAIN_LAST = int(os.getenv("RETAIN_LAST_SNAPSHOTS", "5"))


def table_exists(spark: SparkSession, table: str) -> bool:
    namespace, name = table.split(".")
    try:
        return spark.sql(f"SHOW TABLES IN {CATALOG}.{namespace} LIKE '{name}'").count() > 0
    except Exception:
        return False


def file_stats(spark: SparkSession, table: str) -> dict:
    row = spark.sql(f"""
        SELECT count(*)                                         AS files,
               coalesce(round(avg(file_size_in_bytes) / 1024, 1), 0) AS avg_kb,
               coalesce(sum(record_count), 0)                   AS records
        FROM {CATALOG}.{table}.files
    """).first()
    snapshots = spark.sql(f"SELECT count(*) AS n FROM {CATALOG}.{table}.snapshots").first()["n"]
    return {"files": row["files"], "avg_kb": float(row["avg_kb"]), "records": row["records"], "snapshots": snapshots}


def ts(delta: timedelta) -> str:
    return (datetime.now(timezone.utc) - delta).strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    spark = SparkSession.builder.appName("iceberg-maintenance").getOrCreate()

    for table in [t.strip() for t in TABLES if t.strip()]:
        if not table_exists(spark, table):
            print(f"[maintenance] {table}: no existe todavía, se omite")
            continue

        before = file_stats(spark, table)
        print(f"[maintenance] {table} ANTES   -> {before}")

        spark.sql(f"""
            CALL {CATALOG}.system.rewrite_data_files(
                table   => '{table}',
                strategy => 'binpack',
                options => map(
                    'min-input-files', '2',
                    'target-file-size-bytes', '134217728',
                    'partial-progress.enabled', 'true'
                )
            )
        """).show(truncate=False)

        spark.sql(f"CALL {CATALOG}.system.rewrite_manifests('{table}')").show(truncate=False)

        spark.sql(f"""
            CALL {CATALOG}.system.expire_snapshots(
                table       => '{table}',
                older_than  => TIMESTAMP '{ts(timedelta(minutes=SNAPSHOT_RETENTION_MINUTES))}',
                retain_last => {RETAIN_LAST}
            )
        """).show(truncate=False)

        try:
            spark.sql(f"""
                CALL {CATALOG}.system.remove_orphan_files(
                    table      => '{table}',
                    older_than => TIMESTAMP '{ts(timedelta(hours=ORPHAN_RETENTION_HOURS))}'
                )
            """).show(truncate=False)
        except Exception as exc:  # depende del FileIO/FS disponible; no debe tumbar el resto del mantenimiento
            # Iceberg 1.6 lista huérfanos con el FileSystem de Hadoop, que no conoce el esquema s3:// sin hadoop-aws.
            # En AWS lo resuelve el optimizador de huérfanos del Glue Data Catalog (o hadoop-aws/S3A en el job).
            lines = [line for line in str(exc).splitlines() if "Exception" in line]
            reason = lines[0].strip() if lines else str(exc).splitlines()[0]
            print(f"[maintenance] {table}: remove_orphan_files omitido -> {reason[:200]}")

        after = file_stats(spark, table)
        print(f"[maintenance] {table} DESPUÉS -> {after}")

    spark.stop()


if __name__ == "__main__":
    main()
