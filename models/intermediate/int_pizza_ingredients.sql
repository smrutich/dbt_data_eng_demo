{{
    config(
        materialized='view',
        alias='int_pizza_ingredients'
    )
}}

{#
    Model:      int_pizza_ingredients
    Layer:      Silver (Intermediate)
    Source:     int_pizza_types
    Owner:      Anna
    Description:
        Explodes the comma-separated ingredients string into
        one row per ingredient per pizza type.
        Used to build dim_ingredients and pizza_ingredients
        bridge table in Gold layer.

    Cleaning rules:
        - one row per pizza_type_id + ingredient combination
        - ingredient trimmed of whitespace
        - ingredient_position tracks order in original string
    
    Dependencies: ref('stg_pizza_types')

    Tests:
        - pizza_type_id + ingredient: unique combination
        - pizza_type_id: not_null, relationships to stg_pizza_types
        - ingredient: not_null
        - ingredient_position: not_null
    
    Created Date: 24 March 2025
    Last Modified: 24 March 2025
#}

with source_data as (
    select
        pizza_type_id,
        CATEGORY,
        ingredients,
        SRC_SYS,
       CRT_TS AS SRC_CRT_TS,
        UPD_TS AS SRC_UPD_TS
    from {{ ref('stg_pizza_types') }}
),

exploded as (
    select
        -- primary key
        TRIM(PIZZA_TYPE_ID)::varchar            as pizza_type_id,
        TRIM(CATEGORY)::varchar                 as pizza_category,
        TRIM(value::varchar)                    as ingredient,
        index + 1                               as ingredient_position,
        -- audit columns
        SRC_SYS,
        SRC_CRT_TS,
        SRC_UPD_TS
    from source_data,
    lateral flatten(
        input => split(ingredients, ',')
    )
)

select * from exploded