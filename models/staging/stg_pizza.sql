{{
    config(
        materialized='table',
        on_schema_change='sync_all_columns'
    )
}}

{#
    Model:      bronze_pizza__pizzas
    Layer:      Bronze
    Source:     DBT_TEST_ANNA.PUBLIC.PIZZA
    Owner:      Anna
    Description:
        Raw capture of pizza menu items from the Pizza Place dataset.
        Contains one row per pizza size/type combination (96 rows).
        Materialized as table (not incremental) — static lookup data
        that never changes.

    Dependencies: source('pizza_place', 'PIZZA')

    Audit columns:
        SRC_SYS     - source system identifier
        CRT_TS      - timestamp when dbt first loaded this row
        UPD_TS      - timestamp when dbt last processed this row
    
    Created Date: 24 March 2025
    Last Modified: 24 March 2025
#}

select
    -- primary key
    PIZZA_ID,

    -- source columns (as-is, no transformation)
    PIZZA_TYPE_ID,
    SIZE,
    PRICE,

    -- audit columns
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_UPD_TS,
    {{ audit_columns() }}
    -- 'pizza_place'                       as SRC_SYS,
    -- CURRENT_TIMESTAMP()::TIMESTAMP_NTZ  as CRT_TS,
    -- CURRENT_TIMESTAMP()::TIMESTAMP_NTZ  as UPD_TS

from {{ source('pizza_place', 'PIZZA') }}