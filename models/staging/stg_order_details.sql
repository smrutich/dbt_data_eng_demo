{{
    config(
        materialized='incremental',
        unique_key='order_details_id',
        on_schema_change='sync_all_columns'
    )
}}

{#
    Model:      bronze_stg_order_details
    Layer:      Bronze
    Source:     DBT_TEST_ANNA.PUBLIC.ORDER_DETAILS
    Owner:      Anna
    Description:
        Raw capture of order line items from the Pizza Place dataset.
        Contains one row per pizza per order.
        No transformations applied — audit columns added for lineage only.

    Dependencies: source('pizza_place', 'ORDER_DETAILS')

    Audit columns:
        SRC_SYS     - source system identifier
        CRT_TS      - timestamp when dbt first loaded this row
        UPD_TS      - timestamp when dbt last processed this row

    Created Date: 24 March 2025
    Last Modified: 24 March 2025
#}

select
    -- primary key
    ORDER_DETAILS_ID,

    -- source columns 
    ORDER_ID,
    PIZZA_ID,
    QUANTITY,

    -- audit columns
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_UPD_TS,
    {{ audit_columns() }}

from {{ source('pizza_place', 'ORDER_DETAILS') }}

-- assumes incrmenetal orderIDs
{% if is_incremental() %}
    where ORDER_DETAILS_ID > (select max(ORDER_DETAILS_ID) from {{ this }})
{% endif %}