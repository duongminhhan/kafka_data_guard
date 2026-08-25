{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['order_date', 'order_status'],
        on_schema_change='sync_all_columns',
        views_enabled=false
    )
}}

with changed_dates as (
    select distinct cast(created_at as date) as order_date
    from {{ ref('orders_current') }}
    {% if is_incremental() %}
    where _ingested_at >= (
        select coalesce(
            max(data_updated_at),
            cast(timestamp '1970-01-01 00:00:00' as timestamp(6))
        )
        from {{ this }}
    )
    {% endif %}
)

select
    cast(orders.created_at as date) as order_date,
    orders.order_status,
    count(distinct orders.order_sn) as order_count,
    sum(
        case
            when orders.is_primary_row then orders.total_order_value
            else 0
        end
    ) as total_revenue,
    max(orders._ingested_at) as data_updated_at
from {{ ref('orders_current') }} as orders
inner join changed_dates
    on changed_dates.order_date = cast(orders.created_at as date)
group by
    cast(orders.created_at as date),
    orders.order_status
