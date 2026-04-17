{{
    config(
        materialized='table',
        alias='dim_date',
        tags=['dimension', 'date', 'conformed']
    )
}}

{#
  Model: dim_date.sql
  Description:
    Conformed date dimension generated from a date spine covering
    the full pizza sales period (2015). Includes calendar, fiscal,
    and relative date attributes for slicing and dicing order data.

  Owner: Anna
  Materialization: table
  Refresh Cadence: On demand
  Grain: One row per calendar date

  Dependencies:
    - None (generated via Snowflake GENERATOR function)

  Notes/Assumptions:
    - Date range scoped to 2015-01-01 → 2015-12-31 (pizza dataset year)
    - Fiscal year assumed to start February 1
    - is_weekend uses Snowflake DAYOFWEEK where 0=Sunday, 6=Saturday

  Created Date: 26 March 2025
  Last Modified: 26 March 2025
#}

with date_spine as (
    select
        dateadd(day, seq4(), '2015-01-01'::date) as calendar_date
    from table(generator(rowcount => 365))
),

date_attributes as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['calendar_date']) }}
                                                as date_sk,

        -- natural key
        to_number(
            to_char(calendar_date, 'YYYYMMDD')
        )                                       as date_key,
        calendar_date,

        -- calendar attributes
        dayofweek(calendar_date)                as day_of_week,
        dayofmonth(calendar_date)               as day_of_month,
        dayofyear(calendar_date)                as day_of_year,
        weekofyear(calendar_date)               as week_of_year,
        month(calendar_date)                    as month_number,
        quarter(calendar_date)                  as quarter_number,
        year(calendar_date)                     as year_number,

        -- descriptive attributes
        dayname(calendar_date)                  as day_name,
        monthname(calendar_date)                as month_name,
        left(monthname(calendar_date), 3)       as month_name_short,
        case
            when dayofweek(calendar_date) in (0, 6) then true
            else false
        end                                     as is_weekend,
        case
            when dayofweek(calendar_date) in (0, 6) then false
            else true
        end                                     as is_weekday,

        -- relative date attributes
        datediff(day,   current_date(), calendar_date)      as rltv_day,
        datediff(week,  current_date(), calendar_date)      as rltv_week,
        datediff(month, current_date(), calendar_date)      as rltv_month,
        datediff(quarter, current_date(), calendar_date)    as rltv_quarter,
        datediff(year,  current_date(), calendar_date)      as rltv_year,

        -- fiscal attributes (fiscal year starts Feb 1)
        case
            when month(calendar_date) >= 2 then year(calendar_date)
            else year(calendar_date) - 1
        end                                     as fiscal_year,

        case
            when month(calendar_date) >= 2 then month(calendar_date) - 1
            else month(calendar_date) + 11
        end                                     as fiscal_month,

        ceil(
            case
                when month(calendar_date) >= 2 then month(calendar_date) - 1
                else month(calendar_date) + 11
            end / 3.0
        )                                       as fiscal_quarter,

        -- audit columns
        {{ audit_columns(src_sys='date_generator') }}

    from date_spine
)

select * from date_attributes