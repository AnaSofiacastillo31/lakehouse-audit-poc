-- Consultas de verificación end-to-end (docker compose run --rm spark-sql -f /opt/jobs/verify.sql)

SELECT '1. bronze: eventos crudos'                AS check, count(*) AS n FROM bronze.audit_events;
SELECT '2. bronze: duplicados por event_id'       AS check, count(*) - count(DISTINCT event_id) AS n FROM bronze.audit_events;
SELECT '3. silver: eventos únicos'                AS check, count(*) AS n FROM silver.audit_events;

-- Evolución de esquema: conviven v1 (sin canal) y v2 (con canal)
SELECT schema_version, channel, count(*) AS events
FROM silver.audit_events
GROUP BY schema_version, channel
ORDER BY schema_version, channel;

-- Capa gold
SELECT * FROM gold.daily_audit_summary ORDER BY event_date DESC, events DESC LIMIT 20;

-- Particiones y archivos (small files antes/después del mantenimiento)
SELECT partition, record_count, file_count FROM bronze.audit_events.partitions;

-- Historial ACID: cada micro-batch y cada compactación es un snapshot
SELECT committed_at, snapshot_id, operation, summary['added-data-files'] AS added_files
FROM bronze.audit_events.snapshots
ORDER BY committed_at DESC
LIMIT 10;
