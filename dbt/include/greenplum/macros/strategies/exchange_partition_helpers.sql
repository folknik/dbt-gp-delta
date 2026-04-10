-- ===========================================================================
-- COLUMN HELPERS
-- ===========================================================================

{% macro greenplum__create_partitioned_table_as(temporary, relation, sql) -%}

  {%- set sql_header    = config.get('sql_header', none) -%}
  {%- set unlogged      = config.get('unlogged', false) -%}
  {%- set raw_partition = config.get('raw_partition', none) -%}

  {%- set _contract         = config.get('contract') -%}
  {%- set contract_enforced = _contract.enforced if (_contract is not none) else false -%}

  {{ gp_validate_create_table_config(temporary, unlogged, raw_partition) }}

  {%- set storage_clause = gp_build_storage_clause() -%}
  {%- set dist_clause    = gp_build_distribution_clause() -%}

  {{ sql_header if sql_header is not none }}

  {%- if raw_partition is not none -%}

    {# ------------------------------------------------------------------ #}
    {#  PARTITIONED TABLE: two-step CREATE TABLE + INSERT INTO             #}
    {#  Columns sourced from model contract (schema.yml).                  #}
    {#  CTAS cannot be used for partitioned tables in Greenplum 6.x        #}
    {# ------------------------------------------------------------------ #}
    {{ get_assert_columns_equivalent(sql) }}
    create table {{ relation }}
    {{ get_table_columns_and_constraints() }}
    {{ storage_clause }}
    {{ dist_clause }}
    {{ raw_partition }};

    insert into {{ relation }} (
      {{ sql }}
    );

  {%- elif contract_enforced -%}

    {# ------------------------------------------------------------------ #}
    {#  CONTRACT-ENFORCED NON-PARTITIONED TABLE: CREATE TABLE + INSERT     #}
    {#  Columns sourced from model contract (schema.yml).                  #}
    {# ------------------------------------------------------------------ #}
    {{ get_assert_columns_equivalent(sql) }}
    create
      {%- if temporary %} temporary{%- endif %}
      {%- if unlogged %} unlogged{%- endif %}
    table {{ relation }}
    {{ get_table_columns_and_constraints() }}
    {{ storage_clause }}
    {{ dist_clause }};

    insert into {{ relation }} (
      {{ sql }}
    );

  {%- else -%}

    {# ------------------------------------------------------------------ #}
    {#  NON-PARTITIONED TABLE: standard CTAS                               #}
    {# ------------------------------------------------------------------ #}
    create
      {%- if temporary %} temporary{%- endif %}
      {%- if unlogged %} unlogged{%- endif %}
    table {{ relation }}
    {{ storage_clause }}
    as (
      {{ sql }}
    )
    {{ dist_clause }};

  {%- endif -%}

{%- endmacro %}

{% macro greenplum__get_show_indexes_sql(relation) %}
  select
      i.relname                                   as name,
      m.amname                                    as method,
      ix.indisunique                              as "unique",
      array_to_string(array_agg(a.attname), ',')  as column_names
  from pg_index ix
  join pg_class i      on i.oid = ix.indexrelid
  join pg_am m         on m.oid = i.relam
  join pg_class t      on t.oid = ix.indrelid
  join pg_namespace n  on n.oid = t.relnamespace
  join pg_attribute a  on a.attrelid = t.oid and a.attnum = ANY(ix.indkey)
  where t.relname = '{{ relation.identifier }}'
    and n.nspname = '{{ relation.schema }}'
    and t.relkind in ('r', 'm')
  group by 1, 2, 3
  order by 1, 2, 3
{% endmacro %}


-- ===========================================================================
-- DDL HELPERS
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- gp_build_storage_clause
-- ---------------------------------------------------------------------------
-- Builds the WITH (...) storage clause from model config.
--
-- Config keys read:
--   appendoptimized (bool)  — primary flag; appendonly is legacy alias
--   orientation     (str)   — 'row' or 'column'
--   compresstype    (str)   — e.g. 'zlib', 'zstd', 'quicklz'
--   compresslevel   (int)   — 0–9
--   blocksize       (int)   — bytes
--
-- Returns '' for heap tables (appendoptimized absent or false).
-- ---------------------------------------------------------------------------
{% macro gp_build_storage_clause() %}

  {%- set ao = config.get('appendoptimized', config.get('appendonly', none)) -%}

  {%- if ao is none or ao == false -%}

    {%- set ao_params_set = (
          config.get('orientation',   none) is not none or
          config.get('compresstype',  none) is not none or
          config.get('compresslevel', none) is not none or
          config.get('blocksize',     none) is not none
        ) -%}
    {%- if ao_params_set -%}
      {{ log(
          "WARNING [gp_build_storage_clause]: appendoptimized=false but AO-specific "
          "storage params are set (orientation, compresstype, compresslevel, blocksize). "
          "They will be ignored for a heap table.",
          info=true
      ) }}
    {%- endif -%}
    {{ return('') }}

  {%- else -%}

    {%- set parts = ['appendoptimized=true'] -%}

    {%- set orient = config.get('orientation', none) -%}
    {%- if orient is not none -%}
      {%- set _ = parts.append('orientation=' ~ orient) -%}
    {%- endif -%}

    {%- set ctype = config.get('compresstype', none) -%}
    {%- if ctype is not none -%}
      {%- set _ = parts.append('compresstype=' ~ ctype) -%}
    {%- endif -%}

    {%- set clevel = config.get('compresslevel', none) -%}
    {%- if clevel is not none -%}
      {%- set _ = parts.append('compresslevel=' ~ clevel) -%}
    {%- endif -%}

    {%- set bsize = config.get('blocksize', none) -%}
    {%- if bsize is not none -%}
      {%- set _ = parts.append('blocksize=' ~ bsize) -%}
    {%- endif -%}

    {{ return('WITH (' ~ parts | join(', ') ~ ')') }}

  {%- endif -%}

{% endmacro %}


-- ---------------------------------------------------------------------------
-- gp_build_distribution_clause
-- ---------------------------------------------------------------------------
-- Builds the DISTRIBUTED ... clause from model config.
--
-- Config keys read:
--   distributed_by          (str)  — column name(s), e.g. 'id' or 'tenant_id, id'
--   distributed_replicated  (bool) — DISTRIBUTED REPLICATED
--
-- Default: DISTRIBUTED RANDOMLY.
-- ---------------------------------------------------------------------------
{% macro gp_build_distribution_clause() %}

  {%- set dist_by   = config.get('distributed_by', none) -%}
  {%- set dist_repl = config.get('distributed_replicated', false) -%}

  {%- if dist_by is not none and dist_repl -%}
    {{ exceptions.raise_compiler_error(
        "[gp_build_distribution_clause]: 'distributed_by' and 'distributed_replicated' "
        "cannot both be set. Choose one distribution strategy."
    ) }}
  {%- elif dist_by is not none -%}
    {{ return('DISTRIBUTED BY (' ~ dist_by ~ ')') }}
  {%- elif dist_repl -%}
    {{ return('DISTRIBUTED REPLICATED') }}
  {%- else -%}
    {{ return('DISTRIBUTED RANDOMLY') }}
  {%- endif -%}

{% endmacro %}


-- ---------------------------------------------------------------------------
-- gp_validate_create_table_config
-- ---------------------------------------------------------------------------
{% macro gp_validate_create_table_config(temporary, unlogged, raw_partition) %}

  {%- set _contract         = config.get('contract') -%}
  {%- set contract_enforced = _contract.enforced if (_contract is not none) else false -%}

  {%- if raw_partition is not none and not contract_enforced -%}
    {{ exceptions.raise_compiler_error(
        "[greenplum__create_partitioned_table_as]: 'contract: enforced: true' is required when "
        "'raw_partition' is set. Define column types in schema.yml and add "
        "'contract: {enforced: true}' to the model config."
    ) }}
  {%- endif -%}

  {%- if raw_partition is not none and temporary -%}
    {{ exceptions.raise_compiler_error(
        "[greenplum__create_partitioned_table_as]: Temporary tables cannot be partitioned in Greenplum. "
        "Remove 'raw_partition' or do not use a temporary relation."
    ) }}
  {%- endif -%}

  {%- if raw_partition is not none and unlogged -%}
    {{ exceptions.raise_compiler_error(
        "[greenplum__create_partitioned_table_as]: UNLOGGED tables cannot be partitioned in Greenplum 6. "
        "Remove 'unlogged' or 'raw_partition'."
    ) }}
  {%- endif -%}

{% endmacro %}


-- ===========================================================================
-- PARTITION MANAGEMENT
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- gp_get_delta_window
-- ---------------------------------------------------------------------------
{% macro gp_get_delta_window(relation, partition_key, granularity='day') %}

  {% call statement('gp_get_delta_window', fetch_result=true, auto_begin=false) %}
    SELECT
        to_char(
            date_trunc('{{ granularity }}', min({{ partition_key }}::timestamptz)),
            'YYYY-MM-DD'
        ) AS min_date,
        to_char(
            date_trunc('{{ granularity }}', max({{ partition_key }}::timestamptz)),
            'YYYY-MM-DD'
        ) AS max_date
    FROM {{ relation }}
  {% endcall %}

  {%- set res  = load_result('gp_get_delta_window') -%}
  {%- set rows = res.table.rows -%}
  {%- if rows | length == 0 or rows[0][0] is none -%}
    {{ return({'min_date': none, 'max_date': none}) }}
  {%- else -%}
    {{ return({'min_date': rows[0][0], 'max_date': rows[0][1]}) }}
  {%- endif -%}

{% endmacro %}


-- ---------------------------------------------------------------------------
-- gp_ensure_partition_exists
-- ---------------------------------------------------------------------------
{% macro gp_ensure_partition_exists(target_relation, date_str, next_date_str) %}

  {%- set check_key = 'gp_check_part_' ~ date_str | replace('-','') -%}

  {% call statement(check_key, fetch_result=true, auto_begin=false) %}
    SELECT count(*)
    FROM pg_partitions
    WHERE schemaname = '{{ target_relation.schema }}'
      AND tablename  = '{{ target_relation.identifier }}'
      AND partitiontype = 'range'
      AND partitionrangestart IS NOT NULL
      AND regexp_replace(partitionrangestart, $re$'([^']+)'.*$re$, '\1')::date >= DATE '{{ date_str }}'
      AND regexp_replace(partitionrangestart, $re$'([^']+)'.*$re$, '\1')::date <  DATE '{{ next_date_str }}'
  {% endcall %}

  {%- set check_res      = load_result(check_key) -%}
  {%- set already_exists = (check_res.table.rows[0][0] | int) > 0 -%}

  {%- if not already_exists -%}
    {%- set part_name = 'p_' ~ date_str | replace('-', '_') -%}
    {{ log(
        "gp_ensure_partition_exists: adding partition "
        ~ part_name ~ " to " ~ target_relation,
        info=true
    ) }}
    {% call statement('gp_add_part_' ~ date_str | replace('-','')) %}
      ALTER TABLE {{ target_relation }}
        ADD PARTITION {{ part_name }}
        START (DATE '{{ date_str }}') INCLUSIVE
        END   (DATE '{{ next_date_str }}') EXCLUSIVE;
    {% endcall %}
  {%- else -%}
    {{ log(
        "gp_ensure_partition_exists: partition for "
        ~ date_str ~ " already exists on " ~ target_relation ~ ".",
        info=true
    ) }}
  {%- endif -%}

{% endmacro %}


-- ---------------------------------------------------------------------------
-- gp_create_swap_table
-- ---------------------------------------------------------------------------
{% macro gp_create_swap_table(swap_name, target_relation, date_str, partition_key, source_relation, dist_clause=none, storage_clause=none, merge_mode=false, merge_keys=none, granularity='day') %}

  {%- if dist_clause is none -%}
    {%- set dist_clause = gp_get_target_distribution(target_relation) -%}
  {%- endif -%}
  {%- if storage_clause is none -%}
    {%- set storage_clause = gp_get_target_storage_options(target_relation) -%}
  {%- endif -%}

  {{ log(
      "gp_create_swap_table: " ~ swap_name
      ~ "  mode=" ~ ('merge' if merge_mode else 'overwrite')
      ~ "  dist=[" ~ dist_clause ~ "]"
      ~ "  storage=[" ~ (storage_clause if storage_clause else 'heap') ~ "]",
      info=true
  ) }}

  {% call statement('gp_drop_before_swap_' ~ date_str | replace('-','')) %}
    DROP TABLE IF EXISTS {{ swap_name }};
  {% endcall %}

  {# Build typed SELECT list from the model contract (schema.yml).
     Contract is always enforced for exchange_partition, so model['columns'] is
     the authoritative source of column names and types — no DB round-trip needed. #}
  {%- set contract_cols = model['columns'] -%}
  {%- set typed_select_parts = [] -%}
  {%- set col_names = [] -%}
  {%- for col_name, col in contract_cols.items() -%}
    {%- do typed_select_parts.append(adapter.quote(col_name) ~ '::' ~ col['data_type'] ~ ' AS ' ~ adapter.quote(col_name)) -%}
    {%- do col_names.append(adapter.quote(col_name)) -%}
  {%- endfor -%}
  {%- set typed_select = typed_select_parts | join(',\n      ') -%}
  {%- set col_list = col_names | join(', ') -%}

  {% if not merge_mode %}

    {% call statement('gp_ctas_swap_' ~ date_str | replace('-','')) %}
      CREATE TABLE {{ swap_name }}
      {% if storage_clause %}{{ storage_clause }}{% endif %}
      AS
      SELECT
        {{ typed_select }}
      FROM {{ source_relation }}
      WHERE date_trunc('{{ granularity }}', {{ partition_key }}::timestamptz) = DATE '{{ date_str }}'
      {{ dist_clause }};
    {% endcall %}

  {% else %}

    {%- if merge_keys is none or merge_keys | length == 0 -%}
      {{ exceptions.raise_compiler_error(
          "exchange_partition merge mode requires 'unique_key' config "
          "(a column name or list of column names that uniquely identify a row)."
      ) }}
    {%- endif -%}

    {%- set join_parts = [] -%}
    {%- for key in merge_keys -%}
      {%- do join_parts.append('__trg.' ~ adapter.quote(key) ~ ' = __delta.' ~ adapter.quote(key)) -%}
    {%- endfor -%}
    {%- set join_cond = join_parts | join(' AND ') -%}

    {% call statement('gp_ctas_swap_merge_' ~ date_str | replace('-','')) %}
      CREATE TABLE {{ swap_name }}
      {% if storage_clause %}{{ storage_clause }}{% endif %}
      AS
      SELECT
        {{ typed_select }}
      FROM {{ source_relation }}
      WHERE date_trunc('{{ granularity }}', {{ partition_key }}::timestamptz) = DATE '{{ date_str }}'

      UNION ALL

      SELECT {{ col_list }}
      FROM {{ target_relation }} __trg
      WHERE date_trunc('{{ granularity }}', __trg.{{ partition_key }}::timestamptz) = DATE '{{ date_str }}'
        AND NOT EXISTS (
            SELECT 1
            FROM {{ source_relation }} __delta
            WHERE date_trunc('{{ granularity }}', __delta.{{ partition_key }}::timestamptz) = DATE '{{ date_str }}'
              AND {{ join_cond }}
        )
      {{ dist_clause }};
    {% endcall %}

  {% endif %}

  {% call statement('gp_notnull_cols_' ~ date_str | replace('-',''), fetch_result=true, auto_begin=false) %}
    SELECT a.attname
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = '{{ target_relation.schema }}'
      AND c.relname = '{{ target_relation.identifier }}'
      AND a.attnotnull = true
      AND a.attnum > 0
      AND NOT a.attisdropped
  {% endcall %}

  {%- set nn_rows = load_result('gp_notnull_cols_' ~ date_str | replace('-','')).table.rows -%}
  {%- for row in nn_rows -%}
    {% call statement('gp_set_notnull_' ~ date_str | replace('-','') ~ '_' ~ loop.index) %}
      ALTER TABLE {{ swap_name }} ALTER COLUMN "{{ row[0] }}" SET NOT NULL;
    {% endcall %}
  {%- endfor -%}

{% endmacro %}


-- ---------------------------------------------------------------------------
-- gp_exchange_partition
-- ---------------------------------------------------------------------------
{% macro gp_exchange_partition(target_relation, date_str, swap_name, without_validation=false) %}

  {{ log(
      "gp_exchange_partition: "
      ~ target_relation ~ " FOR DATE '" ~ date_str ~ "'"
      ~ " WITH TABLE " ~ swap_name
      ~ (" WITHOUT VALIDATION" if without_validation else ""),
      info=true
  ) }}

  {% call statement('gp_exchange_' ~ date_str | replace('-','')) %}
    ALTER TABLE {{ target_relation }}
      EXCHANGE PARTITION FOR (DATE '{{ date_str }}')
      WITH TABLE {{ swap_name }}
      {% if without_validation %}WITHOUT VALIDATION{% endif %};
  {% endcall %}

{% endmacro %}


-- ===========================================================================
-- STORAGE INTROSPECTION
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- gp_get_target_distribution
-- ---------------------------------------------------------------------------
{% macro gp_get_target_distribution(relation) %}

  {% call statement('gp_get_target_distribution', fetch_result=true, auto_begin=false) %}
    SELECT
        CASE
            WHEN dp.distkey::text = '' THEN 'DISTRIBUTED RANDOMLY'
            ELSE (
                SELECT 'DISTRIBUTED BY (' ||
                       string_agg('"' || a.attname || '"', ', ' ORDER BY kpos.pos) ||
                       ')'
                FROM (
                    SELECT key::smallint AS attnum,
                           row_number() OVER () AS pos
                    FROM unnest(string_to_array(dp.distkey::text, ' ')) AS key
                ) AS kpos
                JOIN pg_attribute a
                    ON a.attrelid = c.oid
                   AND a.attnum   = kpos.attnum
            )
        END AS dist_clause
    FROM pg_class c
    JOIN pg_namespace n  ON n.oid = c.relnamespace
    JOIN gp_distribution_policy dp ON dp.localoid = c.oid
    WHERE n.nspname = '{{ relation.schema }}'
      AND c.relname = '{{ relation.identifier }}'
    LIMIT 1
  {% endcall %}

  {%- set res  = load_result('gp_get_target_distribution') -%}
  {%- set rows = res.table.rows -%}
  {%- if rows | length > 0 and rows[0][0] is not none -%}
    {{ return(rows[0][0]) }}
  {%- else -%}
    {{ return('DISTRIBUTED RANDOMLY') }}
  {%- endif -%}

{% endmacro %}


-- ---------------------------------------------------------------------------
-- gp_get_target_storage_options
-- ---------------------------------------------------------------------------
{% macro gp_get_target_storage_options(relation) %}

  {% call statement('gp_get_target_storage_options', fetch_result=true, auto_begin=false) %}
    SELECT
        'WITH (' ||
        'appendoptimized=true' ||
        CASE WHEN ao.columnstore
             THEN ', orientation=column'
             ELSE ', orientation=row'
        END ||
        CASE WHEN ao.compresstype IS NOT NULL AND ao.compresstype != ''
             THEN ', compresstype=' || ao.compresstype
             ELSE ''
        END ||
        CASE WHEN ao.compresslevel IS NOT NULL AND ao.compresslevel > 0
             THEN ', compresslevel=' || ao.compresslevel::text
             ELSE ''
        END ||
        CASE WHEN ao.blocksize IS NOT NULL AND ao.blocksize > 0
             THEN ', blocksize=' || ao.blocksize::text
             ELSE ''
        END ||
        ')' AS storage_clause
    FROM pg_class c
    JOIN pg_namespace n  ON n.oid = c.relnamespace
    JOIN pg_appendonly ao ON ao.relid = c.oid
    WHERE n.nspname = '{{ relation.schema }}'
      AND c.relname = '{{ relation.identifier }}'
    LIMIT 1
  {% endcall %}

  {%- set res  = load_result('gp_get_target_storage_options') -%}
  {%- set rows = res.table.rows -%}
  {%- if rows | length > 0 and rows[0][0] is not none -%}
    {{ return(rows[0][0]) }}
  {%- else -%}
    {{ return('') }}
  {%- endif -%}

{% endmacro %}


-- ---------------------------------------------------------------------------
-- gp_assert_table_is_partitioned
-- ---------------------------------------------------------------------------
{% macro gp_assert_table_is_partitioned(target_relation) %}

  {% if not execute %}
    {{ return('') }}
  {% endif %}

  {% call statement('gp_assert_partitioned_check', fetch_result=true, auto_begin=false) %}
    SELECT
        count(*)                                                    AS total,
        sum(CASE WHEN c.relhassubclass THEN 1 ELSE 0 END)          AS is_partitioned
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = '{{ target_relation.schema }}'
      AND c.relname = '{{ target_relation.identifier }}'
      AND c.relkind = 'r'
  {% endcall %}

  {%- set _rows          = load_result('gp_assert_partitioned_check').table.rows[0] -%}
  {%- set already_exists = (_rows[0] | int) > 0 -%}
  {%- set is_partitioned = (_rows[1] | int) > 0 -%}

  {%- if already_exists and not is_partitioned -%}
    {{ exceptions.raise_compiler_error(
        "exchange_partition: " ~ target_relation
        ~ " already exists but is NOT a partitioned table. "
        ~ "Drop it manually to recreate it as a partitioned table."
    ) }}
  {%- endif -%}

{% endmacro %}


-- ===========================================================================
-- RELATION HELPERS
-- ===========================================================================

{% macro greenplum__make_relation_with_suffix(base_relation, suffix, dstring) %}
  {% if dstring %}
    {% set dt = modules.datetime.datetime.now() %}
    {% set dtstring = dt.strftime("%H%M%S%f") %}
    {% set suffix = suffix ~ dtstring %}
  {% endif %}
  {% set suffix_length = suffix | length %}
  {% set relation_max_name_length = base_relation.relation_max_name_length() %}
  {% if suffix_length > relation_max_name_length %}
    {% do exceptions.raise_compiler_error(
        'Relation suffix is too long (' ~ suffix_length ~ ' characters). '
        ~ 'Maximum length is ' ~ relation_max_name_length ~ ' characters.'
    ) %}
  {% endif %}
  {% set identifier = base_relation.identifier[:relation_max_name_length - suffix_length] ~ suffix %}
  {{ return(base_relation.incorporate(path={"identifier": identifier})) }}
{% endmacro %}

{% macro greenplum__make_intermediate_relation(base_relation, suffix) %}
  {{ return(adapter.dispatch('make_relation_with_suffix', 'dbt')(base_relation, suffix, dstring=False)) }}
{% endmacro %}

{% macro greenplum__make_backup_relation(base_relation, backup_relation_type, suffix) %}
  {% set backup_relation = adapter.dispatch('make_relation_with_suffix', 'dbt')(base_relation, suffix, dstring=False) %}
  {{ return(backup_relation.incorporate(type=backup_relation_type)) }}
{% endmacro %}

{% macro greenplum__get_relations() -%}
  {%- call statement('relations', fetch_result=True) -%}
    select distinct
        dependent_namespace.nspname  as dependent_schema,
        dependent_class.relname      as dependent_name,
        referenced_namespace.nspname as referenced_schema,
        referenced_class.relname     as referenced_name
    from pg_class as dependent_class
    join pg_namespace as dependent_namespace
        on dependent_namespace.oid = dependent_class.relnamespace
    join pg_depend as dependent_depend
        on dependent_depend.refobjid = dependent_class.oid
        and dependent_depend.classid   = 'pg_rewrite'::regclass
        and dependent_depend.refclassid = 'pg_class'::regclass
        and dependent_depend.deptype   = 'i'
    join pg_depend as joining_depend
        on joining_depend.objid        = dependent_depend.objid
        and joining_depend.classid     = 'pg_rewrite'::regclass
        and joining_depend.refclassid  = 'pg_class'::regclass
        and joining_depend.refobjid   != dependent_depend.refobjid
    join pg_class as referenced_class
        on referenced_class.oid = joining_depend.refobjid
    join pg_namespace as referenced_namespace
        on referenced_namespace.oid = referenced_class.relnamespace
        and referenced_namespace.nspname != 'information_schema'
        and referenced_namespace.nspname not like 'pg\_%'
    order by dependent_schema, dependent_name, referenced_schema, referenced_name
  {%- endcall -%}
  {{ return(load_result('relations').table) }}
{% endmacro %}


-- ===========================================================================
-- SCHEMA INTROSPECTION
-- ===========================================================================

{% macro greenplum__information_schema_name(database) -%}
  information_schema
{%- endmacro %}

{% macro greenplum__get_catalog_relations(information_schema, relations) -%}
  {%- call statement('catalog', fetch_result=True) -%}
    select
        '{{ information_schema.database }}'     as table_database,
        sch.nspname                             as table_schema,
        tbl.relname                             as table_name,
        case tbl.relkind
            when 'v' then 'VIEW'
            else 'BASE TABLE'
        end                                     as table_type,
        null                                    as table_comment,
        col.attname                             as column_name,
        col.attnum                              as column_index,
        pg_catalog.format_type(col.atttypid, col.atttypmod) as column_type,
        null                                    as column_comment
    from pg_catalog.pg_namespace sch
    join pg_catalog.pg_class tbl     on tbl.relnamespace = sch.oid
    join pg_catalog.pg_attribute col on col.attrelid = tbl.oid
    where tbl.relkind in ('r', 'v', 'm', 'f', 'p')
      and col.attnum > 0
      and not col.attisdropped
      and (
          {%- for relation in relations -%}
              (sch.nspname = '{{ relation.schema }}' and tbl.relname = '{{ relation.identifier }}')
              {%- if not loop.last %} or {% endif -%}
          {%- endfor -%}
      )
    order by sch.nspname, tbl.relname, col.attnum
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
