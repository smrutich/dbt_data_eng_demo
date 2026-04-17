{{
    config(
        materialized='view',
        alias='int_pizza_types'
    )
}}

{#
    Model:      int_pizza_types
    Layer:      Silver (Intermediate)
    Source:     stg_pizza_types
    Owner:      Anna
    Description:
        Type casting, naming standardization and deduplication
        of raw pizza type definitions. No business logic applied.

    Dependencies: ref('stg_pizza_types')

    Tests:
        - pizza_type_id: unique, not_null
        - pizza_name: not_null
        - pizza_category: not_null
    
    Created Date: 24 March 2025
    Last Modified: 24 March 2025
#}

with source_data as (
    select
        PIZZA_TYPE_ID,
        NAME,
        CATEGORY,
        INGREDIENTS,
        SRC_SYS,
       CRT_TS AS SRC_CRT_TS,
        UPD_TS AS SRC_UPD_TS
    from {{ ref('stg_pizza_types') }}
),

cleaned_data as (
    select
        -- primary key
        TRIM(PIZZA_TYPE_ID)::varchar            as pizza_type_id,

        -- type casting and naming standardization only
        TRIM(NAME)::varchar                     as pizza_name,
        TRIM(CATEGORY)::varchar                 as pizza_category,
        TRIM(INGREDIENTS)::varchar              as ingredients,

        -- pass-through audit columns
        SRC_SYS,
        SRC_CRT_TS,
        SRC_UPD_TS

    from source_data
    where PIZZA_TYPE_ID is not null

    qualify row_number() over (
        partition by PIZZA_TYPE_ID
        order by SRC_UPD_TS desc
    ) = 1
)

select * from cleaned_data