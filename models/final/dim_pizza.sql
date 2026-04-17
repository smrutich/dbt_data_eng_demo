{{
    config(
        materialized='table',
        alias='dim_pizza',
        tags=['dimension', 'pizza']
    )
}}

{#
  Model: dim_pizza.sql
  Description:
    Pizza dimension combining pizza SKU attributes (size, price)
    with pizza type attributes (name, category, ingredients).
    Merges int_pizza and int_pizza_types into a single conformed
    dimension for use in fact_orders joins.

  Owner: Anna
  Materialization: table
  Refresh Cadence: On demand
  Grain: One row per pizza SKU (pizza_id + size combination)

  Dependencies:
    - {{ ref('int_pizza') }}
    - {{ ref('int_pizza_types') }}

  Notes/Assumptions:
    - 96 total SKUs (32 pizza types - up to 5 sizes)
    - Size labels standardized: S→Small, M→Medium, L→Large, XL→X-Large, XXL→XX-Large
    - price_tier is a business rule applied here in Gold

  Created Date: 26 March 2025
  Last Modified: 26 March 2025
#}

with pizzas as (
    select * from {{ ref('int_pizza') }}
),

pizza_types as (
    select * from {{ ref('int_pizza_types') }}
),

joined as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.pizza_id']) }} as pizza_sk,
        {{ dbt_utils.generate_surrogate_key(['p.pizza_type_id']) }} as pizza_type_sk,

        -- business keys (kept for lineage/debugging)
        p.pizza_id,
        p.pizza_type_id,

        -- pizza attributes
        pt.pizza_name,
        pt.pizza_category,
        pt.ingredients,
        array_size(split(pt.ingredients, ','))  as ingredient_count,

        -- size standardization 
        -- case p.pizza_size
        --     when 'S'   then 'Small'
        --     when 'M'   then 'Medium'
        --     when 'L'   then 'Large'
        --     when 'XL'  then 'X-Large'
        --     when 'XXL' then 'XX-Large'
        --     else p.pizza_size
        -- end                                     as pizza_size,
        sl.size_label as pizza_size,
        sl.size_description,sl.serves_min,sl.serves_max,

        -- price with business tier classification
        p.price,
        case
            when p.price < 10 then 'Budget'
            when p.price < 15 then 'Standard'
            when p.price < 20 then 'Premium'
            else 'Luxury'
        end                                     as price_tier,

        -- audit columns
        p.SRC_SYS,
        p.SRC_CRT_TS,
        p.SRC_UPD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS

    from pizzas p
    left join pizza_types pt
        using (pizza_type_id)
    left join {{ ref('pizza_size_labels') }} sl
        on p.pizza_size = sl.size_code
)

select * from joined