{{
    config(
        materialized='view',
        alias='int_order_details'
    )
}}

{#
    Model:      int_order_details
    Layer:      Silver (Intermediate)
    Source:     stg_order_details
    Owner:      Anna
    Description:
        Type casting, naming standardization and deduplication
        of raw order details data. 

    Dependencies: ref('stg_order_details')

    Tests:
        - order_detail_id: unique, not_null
        - order_id: not_null, relationships to int_orders
        - pizza_id: not_null, relationships to int_pizza
        - quantity: not_null
    
    Created Date: 24 March 2025
    Last Modified: 24 March 2025
#}

with source_data as (
    select
        ORDER_DETAILS_ID,
        ORDER_ID,
        PIZZA_ID,
        QUANTITY,
        SRC_SYS,
       CRT_TS AS SRC_CRT_TS,
        UPD_TS AS SRC_UPD_TS
    from {{ ref('stg_order_details') }}
),

cleaned_data as (
    select
        -- primary key
        ORDER_DETAILS_ID::number                as order_detail_id,

        -- foreign keys
        ORDER_ID::number                        as order_id,
        TRIM(PIZZA_ID)::varchar                 as pizza_id,

        -- type casting and naming standardization only
        QUANTITY::number                        as quantity,

        -- pass-through audit columns
        SRC_SYS,
        SRC_CRT_TS,
        SRC_UPD_TS

    from source_data
    where ORDER_DETAILS_ID is not null

    qualify row_number() over (
        partition by ORDER_DETAILS_ID
        order by SRC_UPD_TS desc
    ) = 1
)

select * from cleaned_data