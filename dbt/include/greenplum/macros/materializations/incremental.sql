{% macro get_incremental_exchange_partition_sql(arg_dict) %}
  {# greenplum__run_exchange_partition executes all statements directly via statement() calls.
     The incremental materialization expects a SQL string back, so we run the strategy here
     and return a no-op SELECT so that statement("main") has something to execute. #}
  {% do greenplum__run_exchange_partition(staging_relation=arg_dict['temp_relation']) %}
  SELECT 1
{% endmacro %}


{% macro get_incremental_truncate_insert_sql(arg_dict) %}

  {% do return(greenplum__get_truncate_insert_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"])) %}

{% endmacro %}

{% macro greenplum__get_truncate_insert_sql(target, source, dest_columns) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    truncate {{ target }};

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}
