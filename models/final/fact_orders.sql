{{
    config(
        materialized='incremental',
        unique_key='order_detail_sk',
        incremental_strategy='merge',
        alias='fact_orders',
        tags=['fact', 'orders']
    )
}}

{#
  Model: fact_orders.sql
  Description:
    Core fact table capturing every pizza order line item.
    Grain is one row per order detail (one pizza per order line).
    Contains surrogate keys for all dimensions, business keys
    for lineage, and measures for analysis.

    Key metrics available:
      - quantity: number of pizzas ordered
      - unit_price: price per pizza at time of order
      - gross_amount: total revenue per line item (quantity × price)

  Owner: Anna
  Materialization: incremental (merge on order_detail_sk)
  Refresh Cadence: Daily
  Grain: One row per order_detail_id

  Dependencies:
    - {{ ref('int_order_details') }}
    - {{ ref('int_orders') }}

  Notes/Assumptions:
    - No joins to dimensions — surrogate keys generated from business keys
    - quantity_valid nullifies quantities <= 0 (business rule applied here)
    - price_valid nullifies prices <= 0 (business rule applied here)
    - gross_amount uses validated quantity and price

  Created Date: 26 March 2025
  Last Modified: 26 March 2025
#}

with order_details as (
    select * from {{ ref('int_order_details') }}
),

orders as (
    select * from {{ ref('int_orders') }}
),

joined as (
    select
        od.order_detail_id,
        od.order_id,
        od.pizza_id,
        od.quantity,
        o.order_date,
        o.SRC_SYS,
        o.SRC_CRT_TS,
        o.SRC_UPD_TS
    from order_details od
    left join orders o
        using (order_id)
),

final as (
    select
        -- fact surrogate key
        {{ dbt_utils.generate_surrogate_key(['order_detail_id']) }}
                                                as order_detail_sk,

        -- dimension surrogate keys (generated from business keys — no joins)
        {{ dbt_utils.generate_surrogate_key(['pizza_id']) }}
                                                as pizza_sk,
        {{ dbt_utils.generate_surrogate_key(['order_date']) }}
                                                as date_sk,
        {{ dbt_utils.generate_surrogate_key(['order_id']) }}
                                                as order_sk,

        -- business keys (kept for lineage/debugging)
        order_detail_id,
        order_id,
        pizza_id,
        order_date,

        -- measures with business rules applied 
        quantity,
        case
            when quantity <= 0 then null
            else quantity
        end                                     as quantity_valid,

        -- note: price joined from dim_pizza via pizza_sk at query time
        -- gross_amount calculated in reporting layer using dim_pizza.price

        -- audit columns
        SRC_SYS,
        SRC_CRT_TS,
        SRC_UPD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS

    from joined
)

select * from final

{% if is_incremental() %}
    where SRC_CRT_TS > (select max(SRC_CRT_TS) from {{ this }})
{% endif %}