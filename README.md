<p align="center">
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>
<p align="center">
  <a href="https://github.com/dbt-labs/dbt-redshift/actions/workflows/main.yml">
    <img src="https://github.com/dbt-labs/dbt-redshift/actions/workflows/main.yml/badge.svg?event=push" alt="Unit Tests Badge"/>
  </a>
  <a href="https://github.com/dbt-labs/dbt-redshift/actions/workflows/integration.yml">
    <img src="https://github.com/dbt-labs/dbt-redshift/actions/workflows/integration.yml/badge.svg?event=push" alt="Integration Tests Badge"/>
  </a>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-gp-delta

The `dbt-gp-delta` package contains the code enabling dbt to work with Greenplum. It is based on [dbt-gp](https://github.com/vladimir-bm/dbt-gp) - a Greenplum adapter built on top of the [postgres-adapter](https://github.com/dbt-labs/dbt-core/blob/main/plugins/postgres/dbt/include/postgres/profile_template.yml) - and extends it with the `exchange_partition` incremental strategy without modifying any of the original functionality.

## Installation

Easiest way to start use dbt-greenplum is to install it using pip
`pip install dbt-gp-delta==<version>`

Where `<version>` is same as your dbt version

Available versions:
 - 1.4.0

## `exchange_partition` incremental strategy

The `exchange_partition` strategy implements atomic, partition-level incremental loads using Greenplum's `EXCHANGE PARTITION` DDL operation. Instead of inserting rows into the live table, it builds a separate swap table for each affected partition period and atomically replaces the partition via a metadata-only operation - no physical data movement occurs.

#### How it works

> **First run vs subsequent runs.** On the very first run (target table does not exist), dbt creates the partitioned table and immediately loads all data via a direct `INSERT INTO`. No staging or swap tables are used on the first run — the model SQL is executed in full (without the `is_incremental()` filter). From the second run onward, the strategy kicks in: dbt creates a temporary staging table, then uses `EXCHANGE PARTITION` to atomically replace each affected partition via swap tables.

##### Overwrite mode (default, `exchange_merge_partitions=false`)

```
Run 1 — target does not exist:
    ├── CREATE TABLE target  (columns from model contract in schema.yml,
    │                          partition DDL from raw_partition config)
    └── INSERT INTO target (<full model SQL, no is_incremental() filter>)

Run 2+ — target exists:
    │
    ├── if target exists but is NOT partitioned → compile-time error
    │
    ▼
dbt creates a temporary staging table with the model SQL result
    │   (standard dbt incremental mechanism, heap table in the session temp schema)
    ▼
for each period in DISTINCT(staging.partition_key):   ← only periods with data
    ├── ADD PARTITION (if missing)
    ├── CREATE swap table  (schema: exchange_swap_schema, same dist + storage as target)
    │           name: <exchange_swap_schema>.__swap_<model>_<YYYYMMDD>  (day)
    │           name: <exchange_swap_schema>.__swap_<model>_<YYYYMM>    (month)
    │           columns cast to exact types from model contract (schema.yml)
    ├── EXCHANGE PARTITION  (atomic leaf replacement)
    └── DROP swap table
    │
    ▼
dbt drops the temporary staging table automatically (end of session)
ANALYZE target  (if exchange_analyze=true)
```

The target partition is **fully replaced** with data from the delta.

##### Merge mode (`exchange_merge_partitions=true`)

```
Run 1 — target does not exist:
    ├── CREATE TABLE target  (columns from model contract in schema.yml,
    │                          partition DDL from raw_partition config)
    └── INSERT INTO target (<full model SQL, no is_incremental() filter>)

Run 2+ — target exists:
    │
    ├── if target exists but is NOT partitioned → compile-time error
    │
    ▼
dbt creates a temporary staging table with the model SQL result
    │   (standard dbt incremental mechanism, heap table in the session temp schema)
    ▼
for each period in DISTINCT(staging.partition_key):   ← only periods with data
    ├── ADD PARTITION (if missing)
    ├── CREATE swap table AS  (schema: exchange_swap_schema)
    │     SELECT ... FROM delta WHERE period = this_period        ← all delta rows (cast to types from model contract)
    │     UNION ALL
    │     SELECT ... FROM target WHERE period = this_period
    │       AND NOT EXISTS (SELECT 1 FROM delta WHERE merge_keys match)  ← target rows not in delta
    ├── EXCHANGE PARTITION
    └── DROP swap table
    │
    ▼
dbt drops the temporary staging table automatically (end of session)
ANALYZE target  (if exchange_analyze=true)
```

When `merge_keys` match, **delta rows take priority** - target rows whose key matches a delta row are dropped via `NOT EXISTS`. Target rows that are **not** present in the delta by key are **preserved**. Duplicates within the delta itself are **not deduplicated automatically** - if the delta contains multiple rows with the same `merge_key`, all of them will appear in the result. Deduplicate the delta in the model SQL if needed.

#### Required model config

| Parameter | Type | Description |
|---|---|---|
| `partition_column` | string | Column used as the partition key. Must be castable to `timestamptz`. |
| `raw_partition` | string | Full `PARTITION BY RANGE (...) (...)` DDL clause. Written explicitly in `config()` — no auto-generation. |
| `exchange_swap_schema` | string | Schema where swap tables are created. Must exist in the database. |
| `exchange_merge_partitions` | bool | `true` - merge new rows with existing partition data (requires `unique_key`). `false` - fully overwrite affected partitions with staging data. |
| `contract: enforced: true` | - | Declared in `schema.yml` (not in `config()`). Column types are required because the target table is created via explicit DDL, not CTAS. The strategy raises a compile-time error if the contract is not enforced. |

#### Optional model config

| Parameter | Type | Default | Description |
|---|---|---|---|
| `unique_key` | string or list | — | Column(s) that uniquely identify a row. Required when `exchange_merge_partitions=true`. |
| `exchange_partition_granularity` | string | `'day'` | Partition period: `'day'` or `'month'`. |
| `exchange_create_missing_partitions` | bool | `true` | Automatically add missing partitions for new periods found in staging data. |
| `exchange_allow_with_validation` | bool | `true` | When `true`, Greenplum validates that the swap table data satisfies the partition constraints during `EXCHANGE PARTITION`. Set to `false` to skip validation for faster exchange — use only when data correctness is guaranteed upstream. |
| `exchange_analyze` | bool | `true` | Run `ANALYZE` on the target table after all partitions are exchanged. |
| `distributed_by` | string | `RANDOMLY` | Distribution key(s) for the target table, e.g. `'id'` or `'tenant_id, id'`. |
| `appendoptimized` | bool | `true` | Create an append-optimised (AO) table. Set to `false` to create a heap table. |
| `orientation` | string | `'column'` | `'column'` or `'row'`. Applies only when `appendoptimized=true`. |
| `compresstype` | string | `'ZSTD'` | Compression algorithm, e.g. `'ZSTD'`, `'ZLIB'`. Applies only when `appendoptimized=true`. |
| `compresslevel` | int | `4` | Compression level (0–9). Applies only when `appendoptimized=true`. |
| `blocksize` | int | `32768` | Block size in bytes. Applies only when `appendoptimized=true`. |

#### Overwrite mode example

Every run fully replaces all partitions that appear in the new data. Useful when the source already delivers a clean, complete snapshot for each period.

```sql
{% set raw_partition %}
PARTITION BY RANGE (transaction_date) (
    START (DATE '2024-01-01') INCLUSIVE
    END   (DATE '2024-02-01') EXCLUSIVE
    EVERY (INTERVAL '1 day')
)
{% endset %}

{{
    config(
        materialized='incremental',
        incremental_strategy='exchange_partition',

        partition_column='transaction_date',
        raw_partition=raw_partition,
        exchange_partition_granularity='day',
        exchange_swap_schema='stage',
        exchange_merge_partitions=false,

        distributed_by='id',
        appendoptimized=true,
        orientation='column',
        compresstype='ZSTD',
        compresslevel=1
    )
}}

select
    id,
    transaction_date,
    amount
from {{ source('raw', 'transactions') }}
{% if is_incremental() %}
where transaction_date >= current_date - interval '3 days'
{% endif %}
```

`schema.yml` for this model must declare column types:

```yaml
models:
  - name: transactions
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: bigint
      - name: transaction_date
        data_type: date
      - name: amount
        data_type: numeric(18,2)
```

#### Merge mode example

New rows are merged with existing partition data. Rows matched by `unique_key` are replaced by the incoming version; unmatched existing rows are preserved.

```sql
{% set raw_partition %}
PARTITION BY RANGE (event_date) (
    START (DATE '2024-01-01') INCLUSIVE
    END   (DATE '2024-02-01') EXCLUSIVE
    EVERY (INTERVAL '1 month')
)
{% endset %}

{{
    config(
        materialized='incremental',
        incremental_strategy='exchange_partition',

        partition_column='event_date',
        raw_partition=raw_partition,
        exchange_partition_granularity='month',
        exchange_swap_schema='stage',
        exchange_merge_partitions=true,
        unique_key=['tenant_id', 'event_id'],

        distributed_by='tenant_id',
        appendoptimized=true,
        orientation='column',
        compresstype='ZSTD',
        compresslevel=1
    )
}}

select
    tenant_id,
    event_id,
    event_date,
    payload
from {{ source('raw', 'events') }}
{% if is_incremental() %}
where event_date >= date_trunc('month', current_date - interval '1 month')
{% endif %}
```

#### Important notes

- The target table **must be a range-partitioned table**. If a non-partitioned table with the same name already exists, the strategy raises a compile-time error. Drop it manually before the first run.
- `exchange_swap_schema` must exist in the database before the first run. The strategy does not create it automatically.
- `raw_partition` must be provided explicitly in `config()`. There is no auto-generation of the partition DDL.
- Swap tables are named `__swap_{model_name}_{YYYYMMDD}` (day) or `__swap_{model_name}_{YYYYMM}` (month) and are always dropped after a successful exchange. If a run is interrupted they will be cleaned up on the next run. **Model name must not exceed 47 characters** (day granularity) or **49 characters** (month granularity) — Greenplum enforces a 63-character limit on identifiers, and the swap table name prefix `__swap_` (7 chars) plus date suffix `_YYYYMMDD` (9 chars) or `_YYYYMM` (7 chars) consumes the rest.
- The staging table is a standard dbt temporary table created by the incremental materialization before the strategy is called. It lives in the session temp schema and is dropped automatically at the end of the session. It is **not** created in `exchange_swap_schema`.
- `WITHOUT VALIDATION` (`exchange_allow_with_validation=false`) skips Greenplum's constraint check during the exchange. Use it only when you are certain the swap table data satisfies the partition constraints.
- The strategy processes only partition periods that are **actually present in the staging data**. Periods with no new data are never touched, so existing partition data for those periods is preserved as-is.
- If `description` is set on the model or its columns in `schema.yml`, `COMMENT ON TABLE` / `COMMENT ON COLUMN` statements are automatically emitted after table creation.

## Table naming

| Table | Schema | Example name |
|---|---|---|
| **target** (partitioned) | model schema (`profiles.yml` / `dbt_project.yml`) | `marts.orders` |
| **staging** | session temp schema (managed by dbt) | `orders__dbt_tmp` |
| **swap** (day) | `exchange_swap_schema` | `stage.__swap_orders_20240101` |
| **swap** (month) | `exchange_swap_schema` | `stage.__swap_orders_202401` |

The staging table is a standard dbt temp table — created automatically before the strategy runs and dropped at the end of the session. Swap tables live in `exchange_swap_schema` and are dropped immediately after each partition exchange. The `exchange_swap_schema` schema must exist in the database before the first run.

## Auto-creation of the partitioned table

On the first run dbt creates the partitioned target table and immediately loads all data into it via a direct `INSERT INTO`. The full model SQL is executed (without the `is_incremental()` filter):

- Column definitions are taken from the **model contract** (`schema.yml` with `contract: enforced: true` and `data_type` on each column).
- The partition DDL is taken from the `raw_partition` config parameter — written explicitly in `config()`. This gives full control over the initial partition range, step, and any Greenplum-specific partition options. Make sure `raw_partition` covers all periods present in the source data on the first run.
- Further partitions are added on demand via `ALTER TABLE ... ADD PARTITION` as new periods appear in the staging data (controlled by `exchange_create_missing_partitions`).
- Distribution: `DISTRIBUTED BY (<distributed_by>)` or `DISTRIBUTED RANDOMLY` if `distributed_by` is not set.

On subsequent runs the check is idempotent — if the table already exists and is partitioned, creation is skipped and the `EXCHANGE PARTITION` strategy takes over.

If the table exists but is **not** partitioned, the strategy raises a compile-time error with a hint to drop it manually before re-running.

## Usage examples

> **Schema name.** By default dbt constructs the schema name as `<target_schema>_<custom_schema>` (e.g. `public_marts`). This adapter overrides that behaviour via the built-in `generate_schema_name` macro — the table is created exactly in the specified schema (`schema='marts'` → schema `marts`).

A model contract in `schema.yml` is required for all examples:

```yaml
models:
  - name: orders
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: bigint
      - name: event_date
        data_type: date
      - name: user_id
        data_type: integer
      - name: amount
        data_type: numeric(18,4)
      - name: loaded_at
        data_type: timestamp
```

---

### 1. Append-optimised (AO) table

#### 1.1. Overwrite mode

```sql
{% set raw_partition %}
PARTITION BY RANGE (event_date) (
    START (DATE '2026-01-01') INCLUSIVE
    END   (DATE '2026-01-02') EXCLUSIVE
    EVERY (INTERVAL '1 day')
)
{% endset %}

{{
  config(
    schema='marts',
    materialized='incremental',
    incremental_strategy='exchange_partition',
    partition_column='event_date',
    raw_partition=raw_partition,
    exchange_swap_schema='stage',
    exchange_merge_partitions=false,
    exchange_partition_granularity='day',
    distributed_by='id',
    appendoptimized=true,
    orientation='column',
    compresstype='zstd',
    compresslevel=2
  )
}}

SELECT
    id,
    event_date::date AS event_date,
    user_id,
    amount,
    current_timestamp::timestamp AS loaded_at
FROM {{ source("ods", "orders") }}
{% if is_incremental() %}
WHERE event_date::date BETWEEN '{{ var("start_dttm") }}'::date
                             AND '{{ var("end_dttm") }}'::date
{% endif %}
```

The target partition is **fully replaced** with data from the delta.

Generated SQL (first run):
```sql
-- Create target and load all data in one step (first run, is_incremental() is false)
CREATE TABLE marts.orders (
  "id" bigint,
  "event_date" date,
  "user_id" integer,
  "amount" numeric(18,4),
  "loaded_at" timestamp
)
WITH (
  appendoptimized=true,
  orientation=column,
  compresstype=zstd,
  compresslevel=2
)
DISTRIBUTED BY (id)
PARTITION BY RANGE (event_date) (
    START (DATE '2026-01-01') INCLUSIVE
    END   (DATE '2026-01-02') EXCLUSIVE
    EVERY (INTERVAL '1 day')
);

INSERT INTO marts.orders (
  SELECT
      id,
      event_date::date AS event_date,
      user_id,
      amount,
      current_timestamp::timestamp AS loaded_at
  FROM ods.orders
  -- no is_incremental() filter on first run
);
```

Generated SQL (subsequent run, one partition period):
```sql
-- dbt creates a temp staging table automatically (heap, session temp schema):
-- CREATE TEMP TABLE orders__dbt_tmp AS ( <model SQL> ) DISTRIBUTED BY (id);

-- For each period in delta:
-- Column types are taken from model contract (schema.yml) — no DB round-trip needed
CREATE TABLE stage.__swap_orders_20260101
WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=2)
AS
SELECT
  "id"::bigint AS "id",
  "event_date"::date AS "event_date",
  "user_id"::integer AS "user_id",
  "amount"::numeric(18,4) AS "amount",
  "loaded_at"::timestamp AS "loaded_at"
FROM orders__dbt_tmp
WHERE date_trunc('day', event_date::timestamptz) = DATE '2026-01-01'
DISTRIBUTED BY (id);

ALTER TABLE marts.orders
  EXCHANGE PARTITION FOR (DATE '2026-01-01')
  WITH TABLE stage.__swap_orders_20260101;

DROP TABLE stage.__swap_orders_20260101;

-- dbt drops the temp staging table automatically at end of session

ANALYZE marts.orders;
```

---

#### 1.2. Merge mode

```sql
{% set raw_partition %}
PARTITION BY RANGE (event_date) (
    START (DATE '2026-01-01') INCLUSIVE
    END   (DATE '2026-01-02') EXCLUSIVE
    EVERY (INTERVAL '1 day')
)
{% endset %}

{{
  config(
    schema='marts',
    materialized='incremental',
    incremental_strategy='exchange_partition',
    partition_column='event_date',
    raw_partition=raw_partition,
    exchange_swap_schema='stage',
    exchange_merge_partitions=true,
    unique_key=['id'],
    exchange_partition_granularity='day',
    distributed_by='id',
    appendoptimized=true,
    orientation='column',
    compresstype='zstd',
    compresslevel=2
  )
}}

-- Delta only — not a full source snapshot
SELECT
    id,
    event_date::date AS event_date,
    user_id,
    amount,
    current_timestamp::timestamp AS loaded_at
FROM {{ source("ods", "orders") }}
{% if is_incremental() %}
WHERE updated_at BETWEEN '{{ var("start_dttm") }}'::timestamp
                      AND '{{ var("end_dttm") }}'::timestamp
{% endif %}
```

When `id` matches, delta rows win. Target rows not present in the delta are preserved.

Generated SQL (incremental run, one partition period):
```sql
-- dbt creates a temp staging table automatically (heap, session temp schema):
-- CREATE TEMP TABLE orders__dbt_tmp AS ( <model SQL> ) DISTRIBUTED BY (id);

-- For each period in delta:
CREATE TABLE stage.__swap_orders_20260101
WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=2)
AS
-- delta rows (always included, win on key conflict); cast to exact types from model contract (schema.yml)
SELECT
  "id"::bigint AS "id",
  "event_date"::date AS "event_date",
  "user_id"::integer AS "user_id",
  "amount"::numeric(18,4) AS "amount",
  "loaded_at"::timestamp AS "loaded_at"
FROM orders__dbt_tmp
WHERE date_trunc('day', event_date::timestamptz) = DATE '2026-01-01'

UNION ALL

-- target rows not present in delta by merge key (preserved)
SELECT "id", "event_date", "user_id", "amount", "loaded_at"
FROM marts.orders __trg
WHERE date_trunc('day', __trg.event_date::timestamptz) = DATE '2026-01-01'
  AND NOT EXISTS (
      SELECT 1
      FROM orders__dbt_tmp __delta
      WHERE date_trunc('day', __delta.event_date::timestamptz) = DATE '2026-01-01'
        AND __trg."id" = __delta."id"
  )
DISTRIBUTED BY (id);

ALTER TABLE marts.orders
  EXCHANGE PARTITION FOR (DATE '2026-01-01')
  WITH TABLE stage.__swap_orders_20260101;

DROP TABLE stage.__swap_orders_20260101;

-- dbt drops the temp staging table automatically at end of session

ANALYZE marts.orders;
```

---

### 2. Heap table

> **Distribution:** if `distributed_by` is not set, the table is created with `DISTRIBUTED RANDOMLY`.
> `DISTRIBUTED REPLICATED` is not supported: partitioned tables in Greenplum 6 are incompatible with replicated distribution.

#### 2.1. Overwrite mode

```sql
{% set raw_partition %}
PARTITION BY RANGE (event_date) (
    START (DATE '2026-01-01') INCLUSIVE
    END   (DATE '2026-01-02') EXCLUSIVE
    EVERY (INTERVAL '1 day')
)
{% endset %}

{{
  config(
    schema='marts',
    materialized='incremental',
    incremental_strategy='exchange_partition',
    partition_column='event_date',
    raw_partition=raw_partition,
    exchange_swap_schema='stage',
    exchange_merge_partitions=false,
    exchange_partition_granularity='day',
    distributed_by='id',
    appendoptimized=false
  )
}}

SELECT
    id,
    event_date::date AS event_date,
    user_id,
    amount,
    current_timestamp::timestamp AS loaded_at
FROM {{ source("ods", "orders") }}
{% if is_incremental() %}
WHERE event_date::date BETWEEN '{{ var("start_dttm") }}'::date
                             AND '{{ var("end_dttm") }}'::date
{% endif %}
```

Generated SQL (first run):
```sql
-- Create target and load all data in one step (first run, is_incremental() is false)
CREATE TABLE marts.orders (
  "id" bigint,
  "event_date" date,
  "user_id" integer,
  "amount" numeric(18,4),
  "loaded_at" timestamp
)
WITH (appendoptimized=false)
DISTRIBUTED BY (id)
PARTITION BY RANGE (event_date) (
    START (DATE '2026-01-01') INCLUSIVE
    END   (DATE '2026-01-02') EXCLUSIVE
    EVERY (INTERVAL '1 day')
);

INSERT INTO marts.orders (
  SELECT
      id,
      event_date::date AS event_date,
      user_id,
      amount,
      current_timestamp::timestamp AS loaded_at
  FROM ods.orders
  -- no is_incremental() filter on first run
);
```

Generated SQL (subsequent run, one partition period):
```sql
-- dbt creates a temp staging table automatically (heap, session temp schema):
-- CREATE TEMP TABLE orders__dbt_tmp AS ( <model SQL> ) DISTRIBUTED BY (id);

CREATE TABLE stage.__swap_orders_20260101
AS
SELECT
  "id"::bigint AS "id",
  "event_date"::date AS "event_date",
  "user_id"::integer AS "user_id",
  "amount"::numeric(18,4) AS "amount",
  "loaded_at"::timestamp AS "loaded_at"
FROM orders__dbt_tmp
WHERE date_trunc('day', event_date::timestamptz) = DATE '2026-01-01'
DISTRIBUTED BY (id);

ALTER TABLE marts.orders
  EXCHANGE PARTITION FOR (DATE '2026-01-01')
  WITH TABLE stage.__swap_orders_20260101;

DROP TABLE stage.__swap_orders_20260101;

-- dbt drops the temp staging table automatically at end of session

ANALYZE marts.orders;
```

---

#### 2.2. Merge mode

```sql
{% set raw_partition %}
PARTITION BY RANGE (event_date) (
    START (DATE '2026-01-01') INCLUSIVE
    END   (DATE '2026-01-02') EXCLUSIVE
    EVERY (INTERVAL '1 day')
)
{% endset %}

{{
  config(
    schema='marts',
    materialized='incremental',
    incremental_strategy='exchange_partition',
    partition_column='event_date',
    raw_partition=raw_partition,
    exchange_swap_schema='stage',
    exchange_merge_partitions=true,
    unique_key=['id'],
    exchange_partition_granularity='day',
    distributed_by='id',
    appendoptimized=false
  )
}}

-- Delta only — not a full source snapshot
SELECT
    id,
    event_date::date AS event_date,
    user_id,
    amount,
    current_timestamp::timestamp AS loaded_at
FROM {{ source("ods", "orders") }}
{% if is_incremental() %}
WHERE updated_at BETWEEN '{{ var("start_dttm") }}'::timestamp
                      AND '{{ var("end_dttm") }}'::timestamp
{% endif %}
```

Generated SQL (incremental run, one partition period):
```sql
-- dbt creates a temp staging table automatically (heap, session temp schema):
-- CREATE TEMP TABLE orders__dbt_tmp AS ( <model SQL> ) DISTRIBUTED BY (id);

CREATE TABLE stage.__swap_orders_20260101
AS
-- delta rows (always included, win on key conflict); cast to exact types from model contract (schema.yml)
SELECT
  "id"::bigint AS "id",
  "event_date"::date AS "event_date",
  "user_id"::integer AS "user_id",
  "amount"::numeric(18,4) AS "amount",
  "loaded_at"::timestamp AS "loaded_at"
FROM orders__dbt_tmp
WHERE date_trunc('day', event_date::timestamptz) = DATE '2026-01-01'

UNION ALL

-- target rows not present in delta by merge key (preserved)
SELECT "id", "event_date", "user_id", "amount", "loaded_at"
FROM marts.orders __trg
WHERE date_trunc('day', __trg.event_date::timestamptz) = DATE '2026-01-01'
  AND NOT EXISTS (
      SELECT 1
      FROM orders__dbt_tmp __delta
      WHERE date_trunc('day', __delta.event_date::timestamptz) = DATE '2026-01-01'
        AND __trg."id" = __delta."id"
  )
DISTRIBUTED BY (id);

ALTER TABLE marts.orders
  EXCHANGE PARTITION FOR (DATE '2026-01-01')
  WITH TABLE stage.__swap_orders_20260101;

DROP TABLE stage.__swap_orders_20260101;

-- dbt drops the temp staging table automatically at end of session

ANALYZE marts.orders;
```

---

## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

## Join the dbt Community

- Be part of the conversation in the [dbt Community Slack](http://community.getdbt.com/)
- Read more on the [dbt Community Discourse](https://discourse.getdbt.com)

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Let us know on [Slack](http://community.getdbt.com/), or open [an issue](https://github.com/markporoshin/dbt-greenplum/issues/new)
- Want to help us build dbt? Check out the [Contributing Guide](https://github.com/dbt-labs/dbt/blob/HEAD/CONTRIBUTING.md)

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
