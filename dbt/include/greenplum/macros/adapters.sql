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

  {% set partition_spec = config.get('partition_spec', none) %}

  {%- set raw_partition = config.get('raw_partition', none) -%}
  {%- set fields_string = config.get('fields_string', none) -%}

  {%- set default_partition_name = config.get('default_partition_name', default='other') -%}
  {%- set partition_type = config.get('partition_type', none) -%}
  {%- set partition_column = config.get('partition_column', none) -%}
  {%- set partition_start = config.get('partition_start', none) -%}
  {%- set partition_end = config.get('partition_end', none) -%}
  {%- set partition_every = config.get('partition_every', none) -%}
  {%- set partition_values = config.get('partition_values', none) -%}

  {%- set exchange_partition_key = config.get('exchange_partition_key', none) -%}
  {%- if raw_partition is none and partition_type is none and exchange_partition_key is not none and not temporary -%}
    {%- set _granularity = config.get('exchange_partition_granularity', 'day') | lower -%}
    {%- set _start_at    = config.get(
          'exchange_initial_partition_start_at',
          var('exchange_initial_partition_start_at', modules.datetime.datetime.now().strftime('%Y-01-01'))
        ) -%}
    {%- set _dt_start = modules.datetime.datetime.strptime(_start_at, '%Y-%m-%d') -%}
    {%- set _end_at = config.get(
          'exchange_initial_partition_end_at',
          var('exchange_initial_partition_end_at', none)
        ) -%}
    {%- if _end_at is not none -%}
      {%- set _dt_end = modules.datetime.datetime.strptime(_end_at, '%Y-%m-%d') -%}
    {%- elif _granularity == 'day' -%}
      {%- set _dt_end = _dt_start + modules.datetime.timedelta(days=1) -%}
    {%- else -%}
      {%- set _total  = _dt_start.year * 12 + (_dt_start.month - 1) + 1 -%}
      {%- set _dt_end = modules.datetime.datetime(_total // 12, _total % 12 + 1, 1) -%}
    {%- endif -%}
    {%- set raw_partition = 'PARTITION BY RANGE (' ~ exchange_partition_key ~ ') ('
        ~ ' START (DATE \'' ~ _start_at ~ '\') INCLUSIVE'
        ~ ' END   (DATE \'' ~ _dt_end.strftime('%Y-%m-%d') ~ '\') EXCLUSIVE'
        ~ ' EVERY (INTERVAL \'1 ' ~ _granularity ~ '\')'
        ~ ')' -%}
  {%- endif -%}

  {%- set is_partition = raw_partition is not none or partition_type is not none -%}

  {{ sql_header if sql_header is not none }}

  {% if is_partition and not temporary %}

    {# CREATING TABLE #}
    {%- if exchange_partition_key is not none -%}
      {# exchange_partition: колонки из contract, storage без жёстких дефолтов #}
      {{ get_assert_columns_equivalent(sql) }}
      {%- set _fields_ddl    = get_table_columns_and_constraints() -%}
      {%- set _storage_clause = gp_build_storage_clause() -%}
      {%- set _dist_clause    = gp_build_distribution_clause() -%}
    {%- else -%}
      {# стандартный путь dbt-gp: fields_string из конфига #}
      {%- set _fields_ddl    = '(' ~ fields_string ~ ')' -%}
      {%- set _storage_clause = storage_parameters(appendoptimized, blocksize, orientation, compresstype, compresslevel) -%}
      {%- set _dist_clause    = distribution(distributed_by, distributed_replicated) -%}
    {%- endif -%}
    create table if not exists {{ relation }}
    {{ _fields_ddl }}
    {{ _storage_clause }}
    {{ _dist_clause }}
    {{ partitions(raw_partition, partition_type, partition_column,
                  default_partition_name, partition_start, partition_end,
                  partition_every, partition_values) }}
    ;

    {# INSERTING DATA #}
    insert into {{ relation }} (
        {{ sql }}
    );

  {% else %}

      create {% if temporary -%}
        temporary
      {%- elif unlogged -%}
        unlogged
      {%- endif %} table {{ relation }}
      {% if not temporary %}
      {{ storage_parameters(appendoptimized, blocksize, orientation, compresstype, compresslevel) }}
      {% endif %}
      as (
        {{ sql }}
      )
      {{ distribution(distributed_by, distributed_replicated) }}
      ;
  {% endif %}

  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced and config.get('incremental_strategy') != 'exchange_partition' %}
     {{exceptions.warn("Model contracts cannot be enforced by dbt-greenplum!")}}
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


{% macro greenplum_escape_comment(comment) -%}
  {{ return(postgres_escape_comment(comment)) }}
{%- endmacro %}


{% macro greenplum__alter_relation_comment(relation, comment) %}
  {{ return(postgres__alter_relation_comment(relation, comment) ) }}
{% endmacro %}


{% macro greenplum__alter_column_comment(relation, column_dict) %}
  {{ return(postgres__alter_column_comment(relation, column_dict)) }}
{% endmacro %}