/*
  greenplum__run_exchange_partition
  =================================
  Инкрементальная стратегия для Greenplum, основанная на EXCHANGE PARTITION.

  Вызывается через dispatch из postgres-овской материализации incremental
  когда incremental_strategy='exchange_partition'. Точка входа —
  get_incremental_exchange_partition_sql(arg_dict) в incremental.sql,
  которая передаёт уже готовую temp-таблицу как staging_relation.

  Параметры макроса:
    staging_relation (опционально) — если передан, SQL модели не выполняется
      повторно: используется уже готовая таблица (temp-таблица postgres).
      Если none — staging создаётся самостоятельно в exchange_swap_schema.

  Алгоритм:
    1. Читает конфигурацию модели и разрешает relation-объекты.
    2. Валидирует обязательные параметры конфига.
    3. Убеждается что целевая таблица существует и партиционирована.
       Если таблица уже есть, но не партиционирована — выдаёт ошибку.
       (Создание таблицы на первом запуске выполняется в greenplum__create_table_as.)
    4. Если staging_relation не передан — материализует SQL модели
       в staging-таблицу в exchange_swap_schema. Иначе — пропускает.
    5. Определяет диапазон дат (window) из staging-данных.
    6. Для каждого периода, за который staging содержит данные (DISTINCT по partition_key):
       a. При необходимости создаёт недостающую партицию в целевой таблице.
       b. Создаёт swap-таблицу с данными для этого периода.
          В merge-режиме: объединяет данные из staging и целевой партиции,
          дедуплицируя по unique_key (новые данные побеждают).
          В overwrite-режиме: берёт данные только из staging.
       c. Выполняет EXCHANGE PARTITION — атомарно подменяет партицию
          целевой таблицы содержимым swap-таблицы.
       d. Дропает swap-таблицу.
    7. Если staging_relation не был передан — дропает staging-таблицу.
       Иначе — temp-таблица убирается postgres-ом автоматически.
    8. Выполняет ANALYZE на целевой таблице (если не отключён).

  Ответственность вызывающей стороны (incremental.sql):
    — pre/post hooks
    — adapter.commit()
    — return({'relations': [...]})

  Обязательные параметры конфига модели:
    partition_column            — колонка для партиционирования (тип DATE)
    raw_partition               — DDL секции PARTITION BY RANGE (...) (...)
    exchange_swap_schema        — схема для swap-таблиц
    exchange_merge_partitions   — true (merge) / false (overwrite)
    contract: enforced: true    — типы колонок берутся из schema.yml

  Опциональные параметры:
    exchange_partition_granularity      — 'day' (по умолчанию) или 'month'
    exchange_create_missing_partitions  — создавать партиции автоматически (default: true)
    exchange_allow_with_validation      — использовать WITH VALIDATION при обмене (default: true)
    exchange_analyze                    — выполнять ANALYZE после обмена (default: true)
    unique_key                          — ключ дедупликации (обязателен в merge-режиме)
    distributed_by                      — колонка дистрибуции (RANDOMLY если не задана)
    appendoptimized, orientation, compresstype, compresslevel — параметры хранения AO-таблицы
*/

{% macro greenplum__run_exchange_partition(staging_relation=none) %}

  {# ------------------------------------------------------------------ #}
  {#  1. Разрешаем relation-объекты и читаем конфиг модели              #}
  {#                                                                    #}
  {#  target_relation — целевая таблица модели (schema.identifier).     #}
  {#  Все параметры читаются из config() блока модели.                  #}
  {#  unique_key нормализуется в список: 'id' → ['id'].                 #}
  {#  initial_partition_start_at берётся из конфига, затем из vars,     #}
  {#  затем дефолт — 1 января текущего года.                            #}
  {# ------------------------------------------------------------------ #}
  {%- set target_relation    = this.incorporate(type='table') -%}

  {%- set partition_key      = config.get('partition_column') -%}
  {%- set granularity        = config.get('exchange_partition_granularity', 'day') | lower -%}
  {%- set without_validation = not config.get('exchange_allow_with_validation', true) -%}
  {%- set create_missing     = config.get('exchange_create_missing_partitions', true) -%}
  {%- set do_analyze         = config.get('exchange_analyze', true) -%}
  {%- set merge_mode         = config.get('exchange_merge_partitions') -%}
  {%- set _unique_key        = config.get('unique_key', []) -%}
  {%- set merge_keys         = ([_unique_key] if _unique_key is string else _unique_key) -%}
  {%- set dist_key           = config.get('distributed_by', none) -%}
  {%- set stage_schema       = config.get('exchange_swap_schema') -%}

  {%- set _contract         = config.get('contract') -%}
  {%- set contract_enforced = _contract.enforced if (_contract is not none) else false -%}

  {# ------------------------------------------------------------------ #}
  {#  2. Валидация конфига                                              #}
  {#                                                                    #}
  {#  Все обязательные параметры проверяются на этапе компиляции,       #}
  {#  до обращения к базе данных.                                       #}
  {#  contract enforced требуется, чтобы типы колонок были явно         #}
  {#  описаны в schema.yml — они используются при создании таблицы.     #}
  {# ------------------------------------------------------------------ #}
  {% if partition_key is none or partition_key == '' %}
    {{ exceptions.raise_compiler_error(
        "exchange_partition: 'partition_column' is required in model config."
    ) }}
  {% endif %}

  {% if config.get('raw_partition') is none and config.get('partition_type') is none %}
    {{ exceptions.raise_compiler_error(
        "exchange_partition: 'raw_partition' is required in model config. "
        "Provide the full PARTITION BY RANGE (...) (...) clause so the target table "
        "is created as a partitioned table on the first run."
    ) }}
  {% endif %}

  {% if stage_schema is none or stage_schema == '' %}
    {{ exceptions.raise_compiler_error(
        "exchange_partition: 'exchange_swap_schema' is required in model config."
    ) }}
  {% endif %}

  {% if merge_mode is none %}
    {{ exceptions.raise_compiler_error(
        "exchange_partition: 'exchange_merge_partitions' is required in model config. "
        "Set to true for merge mode or false for overwrite mode."
    ) }}
  {% endif %}

  {% if not contract_enforced %}
    {{ exceptions.raise_compiler_error(
        "exchange_partition: 'contract: enforced: true' is required in model config. "
        "Define column types in schema.yml and add 'contract: {enforced: true}' to the model config."
    ) }}
  {% endif %}

  {% if granularity not in ('day', 'month') %}
    {{ exceptions.raise_compiler_error(
        "exchange_partition: granularity must be 'day' or 'month'. Got: " ~ granularity
    ) }}
  {% endif %}

  {% if merge_mode and (merge_keys is none or merge_keys | length == 0) %}
    {{ exceptions.raise_compiler_error(
        "exchange_partition: 'unique_key' is required when "
        "'exchange_merge_partitions' is true. "
        "Specify a column name or list of column names that uniquely identify a row, e.g. "
        "unique_key='id' or unique_key=['tenant_id', 'id']."
    ) }}
  {% endif %}

  {# ------------------------------------------------------------------ #}
  {#  3. Убеждаемся что целевая таблица партиционирована                #}
  {#                                                                    #}
  {#  Вызывается ДО создания staging, чтобы не тратить ресурсы         #}
  {#  на материализацию SQL если таблица некорректна.                  #}
  {#                                                                    #}
  {#  gp_assert_table_is_partitioned:                                   #}
  {#    — таблица партиционирована → no-op                             #}
  {#    — таблица непартиционирована → ошибка компиляции               #}
  {#                                                                    #}
  {#  Создание таблицы на первом запуске выполняется в                  #}
  {#  greenplum__create_table_as (adapters.sql).                        #}
  {# ------------------------------------------------------------------ #}
  {{ gp_assert_table_is_partitioned(target_relation) }}

  {# ------------------------------------------------------------------ #}
  {#  4. Определяем staging-таблицу                                    #}
  {#                                                                    #}
  {#  Если staging_relation передан снаружи (temp-таблица postgres,    #}
  {#  созданная материализацией до вызова стратегии) — используем его. #}
  {#  SQL модели в этом случае уже выполнен, повторно не запускается.  #}
  {#                                                                    #}
  {#  Если staging_relation не передан — создаём его самостоятельно    #}
  {#  в exchange_swap_schema под именем __stage_{model_name}.          #}
  {#  Перед созданием дропается если осталась с предыдущего запуска.   #}
  {# ------------------------------------------------------------------ #}
  {%- set staging_owned = staging_relation is none -%}
  {%- if staging_owned -%}
    {%- set staging_identifier = '__stage_' ~ target_relation.identifier -%}
    {%- set staging_relation   = target_relation.incorporate(path={"schema": stage_schema, "identifier": staging_identifier}) -%}

    {% call statement('drop_staging_pre') %}
      DROP TABLE IF EXISTS {{ staging_relation }};
    {% endcall %}

    {% call statement('main') %}
      {{ greenplum__create_partitioned_table_as(false, staging_relation, sql) }}
    {% endcall %}
  {%- endif -%}

  {# ------------------------------------------------------------------ #}
  {#  5. Определяем диапазон дат (window) из staging-данных             #}
  {#                                                                    #}
  {#  gp_get_delta_window выполняет SELECT MIN/MAX по partition_key     #}
  {#  и возвращает {'min_date': 'YYYY-MM-DD', 'max_date': 'YYYY-MM-DD'}.#}
  {#  Для granularity='month' даты усекаются до первого числа месяца.   #}
  {#                                                                    #}
  {#  Если staging пустой (нет данных за период) — staging дропается    #}
  {#  и стратегия завершается без изменений в целевой таблице.          #}
  {# ------------------------------------------------------------------ #}
  {%- set window   = gp_get_delta_window(staging_relation, partition_key, granularity) -%}
  {%- set min_date = window['min_date'] -%}
  {%- set max_date = window['max_date'] -%}

  {% if min_date is none or max_date is none %}
    {{ log(
        "exchange_partition: staging relation is empty — nothing to exchange.",
        info=true
    ) }}
    {% if staging_owned %}
      {% call statement('drop_staging_empty') %}
        DROP TABLE IF EXISTS {{ staging_relation }};
      {% endcall %}
    {% endif %}
    {{ return('') }}
  {% endif %}

  {{ log(
      "exchange_partition: window " ~ min_date ~ " .. " ~ max_date
      ~ "  mode=" ~ ('merge keys=' ~ merge_keys | join(',') if merge_mode else 'overwrite'),
      info=true
  ) }}

  {# ------------------------------------------------------------------ #}
  {#  6. Итерируем только по периодам, за которые staging содержит      #}
  {#     данные — не по всему диапазону [min_date, max_date].           #}
  {#                                                                    #}
  {#  DISTINCT-запрос по staging гарантирует:                           #}
  {#    — партиция создаётся только под реальный кусок данных           #}
  {#    — пустые дни внутри диапазона не трогаются                      #}
  {#    — EXCHANGE не стирает данные в партициях без delta-строк        #}
  {#                                                                    #}
  {#  Для каждого периода:                                              #}
  {#  a) Вычисляем границы периода (date_str, next_date_str).           #}
  {#     Для month используется целочисленная арифметика месяцев        #}
  {#     (year*12 + month-1), чтобы избежать проблем с концом месяца.   #}
  {#                                                                    #}
  {#  b) Создаём партицию если её нет (exchange_create_missing=true).   #}
  {#                                                                    #}
  {#  c) gp_create_swap_table создаёт временную swap-таблицу            #}
  {#     с той же структурой и дистрибуцией что и целевая таблица.      #}
  {#     Merge-режим: UNION ALL (staging + целевая партиция)            #}
  {#       с дедупликацией через NOT EXISTS по unique_key               #}
  {#       (строки из staging побеждают).                               #}
  {#     Overwrite-режим: только строки из staging за этот период.      #}
  {#                                                                    #}
  {#  d) gp_exchange_partition выполняет:                               #}
  {#     ALTER TABLE target EXCHANGE PARTITION ... WITH TABLE swap      #}
  {#     Это атомарная операция на уровне метаданных — физического      #}
  {#     перемещения данных не происходит. Очень быстро.                #}
  {#     WITHOUT VALIDATION ускоряет обмен (не проверяет ограничения),  #}
  {#     управляется параметром exchange_allow_with_validation.         #}
  {#                                                                    #}
  {#  e) swap-таблица дропается после обмена.                           #}
  {# ------------------------------------------------------------------ #}
  {% call statement('gp_get_staging_periods', fetch_result=true, auto_begin=false) %}
    SELECT DISTINCT
      to_char(
        date_trunc('{{ granularity }}', {{ partition_key }}::timestamptz),
        'YYYY-MM-DD'
      ) AS period_date
    FROM {{ staging_relation }}
    ORDER BY 1
  {% endcall %}

  {%- set staging_periods = load_result('gp_get_staging_periods').table.rows -%}

  {{ log(
      "exchange_partition: " ~ (staging_periods | length) ~ " partition(s) to process.",
      info=true
  ) }}

  {#  Читаем дистрибуцию и параметры хранения из уже существующей      #}
  {#  целевой таблицы (а не из конфига), чтобы swap-таблица была        #}
  {#  совместима с ней для EXCHANGE PARTITION.                           #}
  {%- set target_dist_clause    = gp_get_target_distribution(target_relation) -%}
  {%- set target_storage_clause = gp_get_target_storage_options(target_relation) -%}

  {{ log(
      "exchange_partition: target dist=[" ~ target_dist_clause ~ "]"
      ~ "  storage=[" ~ (target_storage_clause if target_storage_clause else 'heap') ~ "]",
      info=true
  ) }}

  {%- for row in staging_periods -%}

    {%- set date_str   = row[0] -%}
    {%- set current_dt = modules.datetime.datetime.strptime(date_str, '%Y-%m-%d') -%}
    {%- if granularity == 'day' -%}
      {%- set next_dt   = current_dt + modules.datetime.timedelta(days=1) -%}
      {%- set date_safe = current_dt.strftime('%Y%m%d') -%}
    {%- else -%}
      {%- set _total    = current_dt.year * 12 + (current_dt.month - 1) + 1 -%}
      {%- set nxt_year  = _total // 12 -%}
      {%- set nxt_month = _total % 12 + 1 -%}
      {%- set next_dt   = modules.datetime.datetime(nxt_year, nxt_month, 1) -%}
      {%- set date_safe = current_dt.strftime('%Y%m') -%}
    {%- endif -%}

    {%- set next_date_str = next_dt.strftime('%Y-%m-%d') -%}
    {#  swap_fqn: stage_schema.__swap_{model}_{YYYYMMDD или YYYYMM}     #}
    {%- set swap_ident    = '__swap_' ~ target_relation.identifier ~ '_' ~ date_safe -%}
    {%- set swap_fqn      = stage_schema ~ '.' ~ swap_ident -%}

    {{ log(
        "exchange_partition: period " ~ date_str ~ "  swap=" ~ swap_fqn,
        info=true
    ) }}

    {#  a) Создаём партицию если отсутствует                            #}
    {% if create_missing %}
      {{ gp_ensure_partition_exists(target_relation, date_str, next_date_str) }}
    {% endif %}

    {#  b) Создаём swap-таблицу с данными периода                       #}
    {{ gp_create_swap_table(
        swap_fqn,
        target_relation,
        date_str,
        partition_key,
        staging_relation,
        dist_clause=target_dist_clause,
        storage_clause=target_storage_clause,
        merge_mode=merge_mode,
        merge_keys=merge_keys,
        granularity=granularity
    ) }}

    {#  c) Атомарно подменяем партицию через EXCHANGE PARTITION          #}
    {{ gp_exchange_partition(target_relation, date_str, swap_fqn, without_validation) }}

    {#  d) Убираем swap-таблицу                                         #}
    {% call statement('drop_swap_' ~ loop.index) %}
      DROP TABLE IF EXISTS {{ swap_fqn }};
    {% endcall %}

  {%- endfor -%}

  {# ------------------------------------------------------------------ #}
  {#  7. Дропаем staging-таблицу                                        #}
  {#                                                                    #}
  {#  Staging больше не нужна — все периоды обработаны.                 #}
  {# ------------------------------------------------------------------ #}
  {%- if staging_owned -%}
    {% call statement('drop_staging') %}
      DROP TABLE IF EXISTS {{ staging_relation }};
    {% endcall %}
  {%- endif -%}

  {# ------------------------------------------------------------------ #}
  {#  8. ANALYZE                                                        #}
  {#                                                                    #}
  {#  Обновляем статистику планировщика для целевой таблицы.            #}
  {#  Можно отключить через exchange_analyze=false в конфиге модели     #}
  {#  если ANALYZE выполняется отдельно или таблица очень большая.      #}
  {# ------------------------------------------------------------------ #}
  {% if do_analyze %}
    {% call statement('analyze_target') %}
      ANALYZE {{ target_relation }};
    {% endcall %}
  {% endif %}

{% endmacro %}
