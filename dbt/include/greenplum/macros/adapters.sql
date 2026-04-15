{% macro greenplum__create_table_as(temporary, relation, sql) -%}
  {%- set unlogged = config.get('unlogged', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set distributed_replicated = config.get('distributed_replicated', default=false) -%}
  {%- set distributed_by = config.get('distributed_by', none) -%}
  {%- set appendonly = config.get('appendonly', default=true) -%}
  {%- set appendoptimized = config.get('appendoptimized', default=appendonly) -%}
  {%- set orientation = config.get('orientation', default='column') -%}
  {%- set compresstype = config.get('compresstype', default='ZSTD') -%}
  {%- set compresslevel = config.get('compresslevel', default=4) -%}
  {%- set blocksize = config.get('blocksize', default=32768) -%}

  {%- set contract_config   = config.get('contract') -%}
  {%- set contract_enforced = contract_config.enforced if contract_config is not none else false -%}

  {% set partition_spec = config.get('partition_spec', none) %}

  {%- set raw_partition = config.get('raw_partition', none) -%}

  {%- set default_partition_name = config.get('default_partition_name', default='other') -%}
  {%- set partition_type = config.get('partition_type', none) -%}
  {%- set partition_column = config.get('partition_column', none) -%}
  {%- set partition_start = config.get('partition_start', none) -%}
  {%- set partition_end = config.get('partition_end', none) -%}
  {%- set partition_every = config.get('partition_every', none) -%}
  {%- set partition_values = config.get('partition_values', none) -%}

  {%- set is_partition = raw_partition is not none or partition_type is not none -%}

  {{ sql_header if sql_header is not none }}

  {% if is_partition and not temporary %}

    {%- if not contract_enforced -%}
      {{ exceptions.raise_fail_fast_error("Model contract enforced is needed to create table with partitions!") }}
    {%- else -%}
      {{ get_assert_columns_equivalent(sql) }}

      {# CREATING TABLE — отдельный statement, т.к. Greenplum не поддерживает DDL+DML в одном execute() #}
      {% call statement('create_partitioned_table') %}
        create table if not exists {{ relation }}
        {{ get_table_columns_and_constraints() }}
        {{ storage_parameters(appendoptimized, blocksize, orientation, compresstype, compresslevel) }}
        {{ distribution(distributed_by, distributed_replicated) }}
        {{ partitions(raw_partition, partition_type, partition_column,
                      default_partition_name, partition_start, partition_end,
                      partition_every, partition_values) }}
      {% endcall %}

      {{ add_greenplum_comments(relation) }}

      {# INSERTING DATA — отдельный statement #}
      {% call statement('insert_into_partitioned_table') %}
        insert into {{ relation }} (
            {{ sql }}
        )
      {% endcall %}

      SELECT 1
    {%- endif -%}

  {% else %}

      create {% if temporary -%}
        temporary
      {%- elif unlogged -%}
        unlogged
      {%- endif %} table {{ relation }}
      {%- if contract_enforced -%}
        {{ get_assert_columns_equivalent(sql) }}
      {%- endif -%}
      {%- if contract_enforced and not temporary -%}

        {{ get_table_columns_and_constraints() }}
        {{ storage_parameters(appendoptimized, blocksize, orientation, compresstype, compresslevel) }}
        {{ distribution(distributed_by, distributed_replicated) }};
        {{ add_greenplum_comments(relation) }}
        insert into {{ relation }} (
          {{ adapter.dispatch('get_column_names', 'dbt')() }}
        )
        {%- set sql = get_select_subquery(sql) %}

      {%- else %}
        {% if not temporary %}
        {{ storage_parameters(appendoptimized, blocksize, orientation, compresstype, compresslevel) }}
        {% endif %}
        as
      {%- endif %}
      (
        {{ sql }}
      )
      {%- if not contract_enforced or temporary -%}
        {{ distribution(distributed_by, distributed_replicated) }}
      {%- endif -%}
      ;
      {%- if not temporary and not contract_enforced %}
        {{ add_greenplum_comments(relation) }}
      {%- endif %}
  {% endif %}

{%- endmacro %}


{% macro greenplum__get_create_index_sql(relation, index_dict) -%}
  {{ return(postgres__get_create_index_sql(relation, index_dict)) }}
{%- endmacro %}


{% macro greenplum__generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

    {{ default_schema }}

    {%- else -%}

    {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}


{% macro greenplum__create_schema(relation) -%}
  {{ return(postgres__create_schema(relation)) }}
{% endmacro %}


{% macro greenplum__drop_schema(relation) -%}
  {{ return(postgres__drop_schema(relation)) }}
{% endmacro %}


{% macro greenplum__get_columns_in_relation(relation) -%}
  {{ return(postgres__get_columns_in_relation(relation)) }}
{% endmacro %}


{% macro greenplum__list_relations_without_caching(relation) %}
  {{ return(postgres__list_relations_without_caching(relation)) }}
{%- endmacro %}


{% macro greenplum__list_schemas(database) %}
  {{ return(postgres__list_schemas(database)) }}

{% endmacro %}


{% macro greenplum__check_schema_exists(information_schema, schema) -%}
  {{ return(postgres__check_schema_exists(information_schema, schema)) }}
{% endmacro %}


{% macro greenplum__current_timestamp() -%}
  {{ return(postgres__current_timestamp()) }}
{%- endmacro %}

{% macro greenplum__snapshot_string_as_time(timestamp) -%}
    {{ return(postgres__snapshot_string_as_time(timestamp)) }}
{%- endmacro %}


{% macro greenplum__snapshot_get_time() -%}
  {{ return(postgres__snapshot_get_time()) }}
{%- endmacro %}


{% macro greenplum__make_temp_relation(base_relation, suffix) %}
   {{ return(postgres__make_temp_relation(base_relation, suffix)) }}
{% endmacro %}


{% macro add_greenplum_comments(relation) %}
  {%- if model and model.description is defined -%}
    {# Используем текущий контекст модели, если доступен #}

    {# Комментарий к таблице #}
    {%- if model.description -%}
      comment on table {{ relation }} is '{{ model.description | replace("'", "''") | trim }}';
    {%- endif -%}

    {# Комментарии к колонкам #}
    {%- if model.columns is defined -%}
      {%- for column_name in model.columns -%}
        {%- set column_config = model.columns[column_name] -%}
        {%- if column_config.description is defined and column_config.description -%}
          comment on column {{ relation }}.{{ adapter.quote(column_name) }} is '{{ column_config.description | replace("'", "''") | trim }}';
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}

    {{ log("Added comments using current model context for: " ~ relation.name, info=true) }}
  {%- else -%}
    {{ log("Model context not available for comments on: " ~ relation.name, info=true) }}
  {%- endif -%}
{%- endmacro %}


{% macro greenplum_escape_comment(comment) -%}
  {{ return(postgres_escape_comment(comment)) }}
{%- endmacro %}


{% macro greenplum__alter_relation_comment(relation, comment) %}
  {{ return(postgres__alter_relation_comment(relation, comment) ) }}
{% endmacro %}


{% macro greenplum__alter_column_comment(relation, column_dict) %}
  {{ return(postgres__alter_column_comment(relation, column_dict)) }}
{% endmacro %}