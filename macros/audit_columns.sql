{#
  Macro: audit_columns
  Description:
    Generates standard audit columns for all models.
    Ensures consistent naming and data types across
    Bronze, Silver, and Gold layers.

  Arguments:
    src_sys (string): Source system identifier.
                      Defaults to 'pizza_place'.

  Usage:
    {{ audit_columns() }}
    {{ audit_columns(src_sys='date_generator') }}

  Returns:
    SRC_SYS, CRT_TS, UPD_TS columns
#}

{% macro audit_columns(src_sys='pizza_place') %}
    '{{ src_sys }}'                         as SRC_SYS,
    current_timestamp()::timestamp_ntz      as CRT_TS,
    current_timestamp()::timestamp_ntz      as UPD_TS
{% endmacro %}