{{
    config(
        materialized='view',
        alias='int_pizza'
    )
}}

{#
    Model:      int_pizza
    Layer:      Silver (Intermediate)
    Source:     stg_pizza
    Owner:      Anna
    Description:
        Type casting, naming standardization and deduplication
        of raw pizza menu data. No business logic applied.
    
    Dependencies: ref('stg_pizza')

    Tests:
        - pizza_id: unique, not_null
        - pizza_type_id: not_null, relationships to int_pizza_types
        - pizza_size: not_null, accepted_values
        - price: not_null

    Created Date: 24 March 2025
    Last Modified: 24 March 2025
#}

with source_data as (
    select
        PIZZA_ID,
        PIZZA_TYPE_ID,
        SIZE,
        PRICE,
        SRC_SYS,
       CRT_TS AS SRC_CRT_TS,
        UPD_TS AS SRC_UPD_TS
    from {{ ref('stg_pizza') }}
),

cleaned_data as (
    select
        -- primary key
        TRIM(PIZZA_ID)::varchar                 as pizza_id,
        -- foreign key
        TRIM(PIZZA_TYPE_ID)::varchar            as pizza_type_id,

        TRIM(SIZE)::varchar                     as pizza_size,
        PRICE::float                            as price,

        -- pass-through audit columns
        SRC_SYS,
        SRC_CRT_TS,
        SRC_UPD_TS

    from source_data
    where PIZZA_ID is not null

    qualify row_number() over (
        partition by PIZZA_ID
        order by SRC_UPD_TS desc
    ) = 1
)

select * from cleaned_data