{{
    config(
        materialized='table',
        alias='dim_ingredients',
        tags=['dimension', 'ingredients']
    )
}}

{#
  Model: dim_ingredients.sql
  Description:
    Unique ingredients dimension derived from the exploded
    int_pizza_ingredients model. One row per distinct ingredient
    across all pizza types. Used with lookup_pizza_ingredients
    to answer ingredient-level analytics questions.

  Owner: Anna
  Materialization: table
  Refresh Cadence: On demand
  Grain: One row per unique ingredient

  Dependencies:
    - {{ ref('int_pizza_ingredients') }}

  Notes/Assumptions:
    - Ingredients trimmed of whitespace before deduplication
    - ingredient_sk generated from trimmed ingredient name
  
  Created Date: 26 March 2025
  Last Modified: 26 March 2025
#}

with ingredients as (
    select distinct
        trim(ingredient)                        as ingredient,
        SRC_SYS,
        SRC_CRT_TS,
        SRC_UPD_TS
    from {{ ref('int_pizza_ingredients') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['ingredient']) }} as ingredient_sk,

        -- business key
        ingredient,

        -- audit columns
        SRC_SYS,
        SRC_CRT_TS,
        SRC_UPD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS

    from ingredients
)

select * from final