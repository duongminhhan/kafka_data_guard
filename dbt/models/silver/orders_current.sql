{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_row_id',
        on_schema_change='sync_all_columns',
        views_enabled=false
    )
}}

with bronze_orders as (
    select *
    from {{ source('bronze', 'shopee_orders') }}
    {% if is_incremental() %}
    where _ingested_at >= (
        select coalesce(
            max(_ingested_at),
            cast(timestamp '1970-01-01 00:00:00' as timestamp(6))
        )
        from {{ this }}
    )
    {% endif %}
),

ranked_orders as (
    select
        *,
        row_number() over (
            partition by pkId
            order by _ingested_at desc, synced_at desc
        ) as ingestion_rank
    from bronze_orders
)

select
    pkId as order_row_id,
    order_sn,
    user_id,
    shop_id,
    item_id,
    item_name,
    model_id,
    model_name,
    quantity,
    cast(original_price as decimal(18, 2)) as original_price,
    cast(total_order_value as decimal(18, 2)) as total_order_value,
    upper(trim(order_status)) as order_status,
    cast(create_time as timestamp(6)) as created_at,
    cast(completed_time as timestamp(6)) as completed_at,
    try_cast(is_primary_row as boolean) as is_primary_row,
    cast(synced_at as timestamp(6)) as source_synced_at,
    _batch_id,
    _ingested_at,
    _source_system,
    _source_table
from ranked_orders
where ingestion_rank = 1
