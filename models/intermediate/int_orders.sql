{{
    config(
        materialized='view',
        alias='int_orders'
    )
}}

{#
    Model:      int_orders
    Layer:      Silver (Intermediate)
    Source:     stg_orders
    Owner:      Anna
    Description:
        Type casting, naming standardization and deduplication
        of raw orders data. No business logic applied.
    
    Dependencies: ref('stg_orders')

    Tests:
        - order_id: unique, not_null
        - order_date: not_null
        - order_time: not_null
    
    Created Date: 24 March 2025
    Last Modified: 24 March 2025
#}

with source_data as (
    select
        ORDER_ID,
        DATE,
        TIME,
        SRC_SYS,
        --- timestamp -- capture the timestamp when bronze data is loaded
        CRT_TS AS SRC_CRT_TS,
        UPD_TS AS SRC_UPD_TS
    from {{ ref('stg_orders') }}
),

cleaned_data as (
    select
        -- primary key
        ORDER_ID::number                        as order_id,

        -- type casting and naming standardization only
        DATE::date                              as order_date,
        TIME::time                              as order_time,

        -- pass-through audit columns
        SRC_SYS,
        SRC_CRT_TS,
        SRC_UPD_TS

    from source_data
    where ORDER_ID is not null

    qualify row_number() over (
        partition by ORDER_ID
        order by SRC_UPD_TS desc
    ) = 1
)

select * from cleaned_data