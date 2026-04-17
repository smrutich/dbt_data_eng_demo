{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='sync_all_columns'
    )
}}

{#
    Model:      bronze_pizza__orders
    Layer:      Bronze
    Source:     DBT_TEST_ANNA.PUBLIC.ORDERS
    Owner:      Anna
    Description:
        Raw capture of pizza orders from the Pizza Place dataset.
        Contains one row per order with date and time of purchase.
        No transformations applied — audit columns added for lineage only.

    Dependencies: source('pizza_place', 'ORDERS')
    
    Audit columns:
        SRC_SYS     - source system identifier
        CRT_TS      - timestamp when dbt first loaded this row
        UPD_TS      - timestamp when dbt last processed this row
    
    Created Date: 24 March 2025
    Last Modified: 24 March 2025
#}

select
    -- primary key
    ORDER_ID,

    -- source columns (as-is, no transformation)
    DATE,
    TIME,

    -- audit columns
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_UPD_TS,
    {{ audit_columns() }}

from {{ source('pizza_place', 'ORDERS') }}

{% if is_incremental() %}
    where DATE > (select max(DATE) from {{ this }})
{% endif %}