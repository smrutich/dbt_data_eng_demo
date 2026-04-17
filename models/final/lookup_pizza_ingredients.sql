{{
    config(
        materialized='table',
        alias='lookup_pizza_ingredients',
        tags=['bridge', 'ingredients', 'pizza']
    )
}}

{#
  Model: lookup_pizza_ingredients.sql
  Description:
    Bridge table resolving the many-to-many relationship between
    dim_pizza and dim_ingredients. One row per pizza_type +
    ingredient combination. Enables ingredient-level analysis
    by joining through this table from fact_orders.

  Owner: Anna
  Materialization: table
  Refresh Cadence: On demand
  Grain: One row per pizza_type_id + ingredient combination

  Dependencies:
    - {{ ref('int_pizza_ingredients') }}
    - {{ ref('dim_ingredients') }}
    - {{ ref('dim_pizza') }}

  Notes/Assumptions:
    - Joins on pizza_type_id not pizza_id (ingredients defined at type level)
    - ingredient_position preserves original ordering from source string

  Created Date: 26 March 2025
  Last Modified: 26 March 2025
#}

with pizza_ingredients as (
    select * from {{ ref('int_pizza_ingredients') }}
),

dim_ingredients as (
    select * from {{ ref('dim_ingredients') }}
),

dim_pizza as (
    select distinct
        pizza_type_id
    from {{ ref('dim_pizza') }}
),

final as (
    select
        -- surrogate keys
        {{ dbt_utils.generate_surrogate_key(['pi.pizza_type_id', 'di.ingredient']) }} as pizza_ingredient_sk,
        {{ dbt_utils.generate_surrogate_key(['pi.pizza_type_id']) }} as pizza_type_sk,
        {{ dbt_utils.generate_surrogate_key(['pi.ingredient']) }}    as ingredient_sk,

        -- business keys (kept for lineage/debugging)
        pi.pizza_type_id,
        di.ingredient,
        pi.ingredient_position,

        -- audit columns
        current_timestamp()::timestamp_ntz      as CRT_TS,
        current_timestamp()::timestamp_ntz      as UPD_TS,
        pi.SRC_SYS,
        pi.SRC_CRT_TS,
        pi.SRC_UPD_TS

    from pizza_ingredients pi
    left join dim_ingredients di
        on trim(pi.ingredient) = trim(di.ingredient)
    left join dim_pizza dp
        on pi.pizza_type_id = dp.pizza_type_id
)

select * from final