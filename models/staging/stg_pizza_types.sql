{{
    config(
        materialized='table',
        on_schema_change='sync_all_columns'
    )
}}

{#
    Model:      bronze_pizza__pizza_types
    Layer:      Bronze
    Source:     DBT_TEST_ANNA.PUBLIC.PIZZA_TYPES
    Owner:      Anna
    Description:
        Raw capture of pizza type definitions from the Pizza Place dataset.
        Contains one row per pizza type (32 rows) with name, category,
        and raw comma-separated ingredients string.
        Materialized as table (not incremental) — static lookup data
        that never changes.

    Dependencies: source('pizza_place', 'PIZZA_TYPES')

    Audit columns:
        SRC_SYS     - source system identifier
        CRT_TS      - timestamp when dbt first loaded this row
        UPD_TS      - timestamp when dbt last processed this row
    
    Created Date: 24 March 2025
    Last Modified: 24 March 2025
#}

select
    -- primary key
    PIZZA_TYPE_ID,

    -- source columns (as-is, no transformation)
    NAME,
    CATEGORY,
    INGREDIENTS,

    -- audit columns
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_UPD_TS,
    {{ audit_columns() }}

from {{ source('pizza_place', 'PIZZA_TYPES') }}