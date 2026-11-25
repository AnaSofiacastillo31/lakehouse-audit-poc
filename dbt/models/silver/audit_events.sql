{#-
  Silver: un registro por evento, tipado y con el payload interpretado según schema_version.
  - MERGE por event_id: idempotente frente a duplicados del outbox y a re-ejecuciones de dbt.
  - Incremental por ingested_at (con >= para no perder filas en el borde del lote anterior).
  - on_schema_change: si se agrega una columna (p. ej. un campo de la v3), se añade a la tabla Iceberg
    sin reescribir datos (Iceberg rastrea columnas por id, no por posición).
-#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'event_id',
    partition_by = ['days(occurred_at)'],
    on_schema_change = 'append_new_columns',
    tblproperties = {
        'format-version': '2',
        'write.parquet.compression-codec': 'zstd',
        'write.merge.mode': 'copy-on-write'
    }
) }}

with source_events as (

    select *
    from {{ source('bronze', 'audit_events') }}
    where event_id is not null

    {% if is_incremental() %}
      and ingested_at >= (select coalesce(max(ingested_at), timestamp '1970-01-01') from {{ this }})
    {% endif %}

),

deduplicated as (

    select
        *,
        row_number() over (partition by event_id order by kafka_partition, kafka_offset) as rn
    from source_events

)

select
    event_id,
    aggregate_type,
    aggregate_id,
    event_type,
    schema_version,
    occurred_at,
    cast(get_json_object(payload, '$.transferId') as bigint)     as transfer_id,
    get_json_object(payload, '$.sourceAccount')                  as source_account,
    get_json_object(payload, '$.targetAccount')                  as target_account,
    cast(get_json_object(payload, '$.amount') as decimal(18, 2)) as amount,
    get_json_object(payload, '$.currency')                       as currency,
    -- v1 no tenía canal: se normaliza en vez de dejar nulos ambiguos
    case
        when schema_version >= 2 then get_json_object(payload, '$.channel')
        else 'NOT_CAPTURED'
    end                                                          as channel,
    payload                                                      as payload_raw,
    ingested_at
from deduplicated
where rn = 1
