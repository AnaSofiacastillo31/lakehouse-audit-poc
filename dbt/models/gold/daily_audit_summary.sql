{#- Gold: agregado diario para reportes regulatorios y tableros. Se recalcula completo (barato a este nivel). -#}
{{ config(
    materialized = 'table',
    tblproperties = {'format-version': '2'}
) }}

select
    cast(occurred_at as date)        as event_date,
    event_type,
    channel,
    currency,
    count(*)                         as events,
    sum(amount)                      as total_amount,
    count(distinct source_account)   as distinct_source_accounts,
    max(ingested_at)                 as last_ingested_at
from {{ ref('audit_events') }}
group by 1, 2, 3, 4
