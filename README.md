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
 - 0.4.0

## `exchange_partition` incremental strategy

The `exchange_partition` strategy implements atomic, partition-level incremental loads using Greenplum's `EXCHANGE PARTITION` DDL operation. Instead of inserting rows into the live table, it builds a separate swap table for each affected partition period and atomically replaces the partition via a metadata-only operation — no physical data movement occurs.

#### How it works

##### Overwrite mode (default, `exchange_merge_partitions=false`)

```
Start model
    │
    ▼
if target does not exist:
    └── CREATE TABLE target  (columns from model contract in schema.yml,
    │                          PARTITION BY RANGE, EVERY '1 day|month')
    │
    ├── if target exists but is NOT partitioned → compile-time error
    │
    ▼
DROP staging IF EXISTS  (idempotency on re-run after failure)
    │
    ▼
CREATE staging table  (one per run, schema: exchange_stage_schema)
    │                  name: <exchange_stage_schema>.__stage_<model>
    ▼
for each period in DISTINCT(staging.partition_key):   ← only periods with data
    ├── ADD PARTITION (if missing)
    ├── CREATE swap table  (schema: exchange_stage_schema, same dist + storage as target)
    │           name: <exchange_stage_schema>.__swap_<model>_<YYYYMMDD>  (day)
    │           name: <exchange_stage_schema>.__swap_<model>_<YYYYMM>    (month)
    ├── INSERT  ← only staging rows for this period
    ├── EXCHANGE PARTITION  (atomic leaf replacement)
    └── DROP swap table
    │
    ▼
DROP staging table
ANALYZE target  (if exchange_analyze=true)
```

The target partition is **fully replaced** with data from the delta.

##### Merge mode (`exchange_merge_partitions=true`)

```
Start model
    │
    ▼
if target does not exist:
    └── CREATE TABLE target  (columns from model contract in schema.yml,
    │                          PARTITION BY RANGE, EVERY '1 day|month')
    │
    ├── if target exists but is NOT partitioned → compile-time error
    │
    ▼
DROP staging IF EXISTS  (idempotency on re-run after failure)
    │
    ▼
CREATE staging table  (one per run, schema: exchange_stage_schema)
    │                  name: <exchange_stage_schema>.__stage_<model>
    ▼
for each period in DISTINCT(staging.partition_key):   ← only periods with data
    ├── ADD PARTITION (if missing)
    ├── CREATE swap table AS  (schema: exchange_stage_schema)
    │     SELECT ... FROM delta WHERE period = this_period        ← all delta rows
    │     UNION ALL
    │     SELECT ... FROM target WHERE period = this_period
    │       AND NOT EXISTS (SELECT 1 FROM delta WHERE merge_keys match)  ← target rows not in delta
    ├── EXCHANGE PARTITION
    └── DROP swap table
    │
    ▼
DROP staging table
ANALYZE target  (if exchange_analyze=true)
```

When `merge_keys` match, **delta rows take priority** — target rows whose key matches a delta row are dropped via `NOT EXISTS`. Target rows that are **not** present in the delta by key are **preserved**. Duplicates within the delta itself are **not deduplicated automatically** — if the delta contains multiple rows with the same `merge_key`, all of them will appear in the result. Deduplicate the delta in the model SQL if needed.

#### Required model config

| Parameter | Type | Description |
|---|---|---|
| `exchange_partition_key` | string | Column used as the partition key. Must be castable to `timestamptz`. |
| `exchange_stage_schema` | string | Schema where staging and swap tables are created. Must exist in the database. |
| `exchange_merge_partitions` | bool | `true` — merge new rows with existing partition data (requires `unique_key`). `false` — fully overwrite affected partitions with staging data. |
| `contract: enforced: true` | — | Declared in `schema.yml` (not in `config()`). Column types are required because the target table is created via explicit DDL, not CTAS. The strategy raises a compile-time error if the contract is not enforced. |

#### Optional model config

| Parameter | Type | Default | Description |
|---|---|---|---|
| `unique_key` | string or list | — | Column(s) that uniquely identify a row. Required when `exchange_merge_partitions=true`. |
| `exchange_partition_granularity` | string | `'day'` | Partition period: `'day'` or `'month'`. |
| `exchange_create_missing_partitions` | bool | `true` | Automatically add missing partitions for new periods found in staging data. |
| `exchange_allow_with_validation` | bool | `true` | Use `WITH VALIDATION` during `EXCHANGE PARTITION`. Set to `false` for faster exchange when data correctness is guaranteed upstream. |
| `exchange_analyze` | bool | `true` | Run `ANALYZE` on the target table after all partitions are exchanged. |
| `exchange_initial_partition_start_at` | string `YYYY-MM-DD` | Jan 1 of current year | Start date of the initial partition created on the first run. Can also be set via dbt var `exchange_initial_partition_start_at`. |
| `exchange_initial_partition_end_at` | string `YYYY-MM-DD` | start + 1 period | End date of the initial partition range. If omitted, exactly one partition is created. Can also be set via dbt var `exchange_initial_partition_end_at`. |
| `distributed_by` | string | `RANDOMLY` | Distribution key(s) for the target table, e.g. `'id'` or `'tenant_id, id'`. |
| `appendoptimized` | bool | — | Create an append-optimised (AO) table. |
| `orientation` | string | — | `'column'` or `'row'`. Applies only when `appendoptimized=true`. |
| `compresstype` | string | — | Compression algorithm, e.g. `'ZSTD'`, `'ZLIB'`. Applies only when `appendoptimized=true`. |
| `compresslevel` | int | — | Compression level (0–9). Applies only when `appendoptimized=true`. |
| `blocksize` | int | — | Block size in bytes. Applies only when `appendoptimized=true`. |

#### Overwrite mode example

Every run fully replaces all partitions that appear in the new data. Useful when the source already delivers a clean, complete snapshot for each period.

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='exchange_partition',

        exchange_partition_key='transaction_date',
        exchange_partition_granularity='day',
        exchange_stage_schema='stage',
        exchange_merge_partitions=false,

        exchange_initial_partition_start_at='2024-01-01',
        exchange_initial_partition_end_at='2024-02-01',

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
{{
    config(
        materialized='incremental',
        incremental_strategy='exchange_partition',

        exchange_partition_key='event_date',
        exchange_partition_granularity='month',
        exchange_stage_schema='stage',
        exchange_merge_partitions=true,
        unique_key=['tenant_id', 'event_id'],

        exchange_initial_partition_start_at='2024-01-01',

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
- `exchange_stage_schema` must exist in the database before the first run. The strategy does not create it automatically.
- Swap tables are named `__swap_{model_name}_{YYYYMMDD}` (day) or `__swap_{model_name}_{YYYYMM}` (month) and are always dropped after a successful exchange. If a run is interrupted they will be cleaned up on the next run. **Model name must not exceed 47 characters** (day granularity) or **49 characters** (month granularity) — Greenplum enforces a 63-character limit on identifiers, and the swap table name prefix `__swap_` (7 chars) plus date suffix `_YYYYMMDD` (9 chars) or `_YYYYMM` (7 chars) consumes the rest.
- Staging tables are named `__stage_{model_name}` and live in `exchange_stage_schema`. They are also dropped after a successful run.
- `WITHOUT VALIDATION` (`exchange_allow_with_validation=false`) skips Greenplum's constraint check during the exchange. Use it only when you are certain the swap table data satisfies the partition constraints.
- The strategy processes only partition periods that are **actually present in the staging data**. Periods with no new data are never touched, so existing partition data for those periods is preserved as-is.

## Table naming

| Table | Schema | Example name |
|---|---|---|
| **target** (partitioned) | model schema (`profiles.yml` / `dbt_project.yml`) | `marts.orders` |
| **staging** | `exchange_stage_schema` | `stage.__stage_orders` |
| **swap** (day) | `exchange_stage_schema` | `stage.__swap_orders_20240101` |
| **swap** (month) | `exchange_stage_schema` | `stage.__swap_orders_202401` |

Staging and swap tables are **temporary** — they are dropped at the end of each run. The `exchange_stage_schema` schema must exist in the database before the first run.

## Auto-creation of the partitioned table

On the first run the strategy automatically creates the target table as a `PARTITION BY RANGE` table — in both overwrite and merge modes:

- Column definitions are taken from the **model contract** (`schema.yml` with `contract: enforced: true` and `data_type` on each column).
- The initial partition range is defined by `exchange_initial_partition_start_at` and `exchange_initial_partition_end_at`. If `exchange_initial_partition_end_at` is omitted, a single partition of one period (one day or one month) is created. Further partitions are added on demand via `ALTER TABLE ... ADD PARTITION`.
- Partition step: `EVERY (INTERVAL '1 day')` or `EVERY (INTERVAL '1 month')` — controlled by `exchange_partition_granularity`.
- Distribution: `DISTRIBUTED BY (<distributed_by>)` or `DISTRIBUTED RANDOMLY` if `distributed_by` is not set.

On subsequent runs the check is idempotent — if the table already exists and is partitioned, creation is skipped.

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
{{
  config(
    schema='marts',
    materialized='incremental',
    incremental_strategy='exchange_partition',
    exchange_partition_key='event_date',
    exchange_stage_schema='stage',
    exchange_merge_partitions=false,
    exchange_partition_granularity='day',
    exchange_initial_partition_start_at='2026-01-01',
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

Generated SQL (first run + one partition period):
```sql
-- Create target (first run)
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

-- Idempotency: drop staging left from a previous failed run
DROP TABLE IF EXISTS stage.__stage_orders;

-- Create staging (one per run)
CREATE TABLE stage.__stage_orders (
  "id" bigint,
  "event_date" date,
  "user_id" integer,
  "amount" numeric(18,4),
  "loaded_at" timestamp
)
WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=2)
DISTRIBUTED BY (id);

INSERT INTO stage.__stage_orders (
    SELECT
        id,
        event_date::date AS event_date,
        user_id,
        amount,
        current_timestamp::timestamp AS loaded_at
    FROM orders
    WHERE event_date::date BETWEEN '2026-01-01'::date AND '2026-01-02'::date
);

-- For each period in delta:
CREATE TABLE stage.__swap_orders_20260101
WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=2)
AS
SELECT id, event_date, user_id, amount, loaded_at
FROM stage.__stage_orders
WHERE date_trunc('day', event_date::timestamptz) = DATE '2026-01-01'
DISTRIBUTED BY (id);

ALTER TABLE marts.orders
  EXCHANGE PARTITION FOR (DATE '2026-01-01')
  WITH TABLE stage.__swap_orders_20260101;

DROP TABLE stage.__swap_orders_20260101;

-- Final cleanup
DROP TABLE stage.__stage_orders;

ANALYZE marts.orders;
```

---

#### 1.2. Merge mode

```sql
{{
  config(
    schema='marts',
    materialized='incremental',
    incremental_strategy='exchange_partition',
    exchange_partition_key='event_date',
    exchange_stage_schema='stage',
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
-- Idempotency: drop staging left from a previous failed run
DROP TABLE IF EXISTS stage.__stage_orders;

-- Create staging (one per run)
CREATE TABLE stage.__stage_orders (
  "id" bigint,
  "event_date" date,
  "user_id" integer,
  "amount" numeric(18,4),
  "loaded_at" timestamp
)
WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=2)
DISTRIBUTED BY (id);

INSERT INTO stage.__stage_orders (
    SELECT
        id,
        event_date::date AS event_date,
        user_id,
        amount,
        current_timestamp::timestamp AS loaded_at
    FROM orders
    WHERE updated_at BETWEEN '2026-01-01 00:00:00'::timestamp
                          AND '2026-01-02 00:00:00'::timestamp
);

-- For each period in delta:
CREATE TABLE stage.__swap_orders_20260101
WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=2)
AS
-- delta rows (always included, win on key conflict)
SELECT id, event_date, user_id, amount, loaded_at
FROM stage.__stage_orders
WHERE date_trunc('day', event_date::timestamptz) = DATE '2026-01-01'

UNION ALL

-- target rows not present in delta by merge key (preserved)
SELECT id, event_date, user_id, amount, loaded_at
FROM marts.orders __trg
WHERE date_trunc('day', __trg.event_date::timestamptz) = DATE '2026-01-01'
  AND NOT EXISTS (
      SELECT 1
      FROM stage.__stage_orders __delta
      WHERE date_trunc('day', __delta.event_date::timestamptz) = DATE '2026-01-01'
        AND __trg."id" = __delta."id"
  )
DISTRIBUTED BY (id);

ALTER TABLE marts.orders
  EXCHANGE PARTITION FOR (DATE '2026-01-01')
  WITH TABLE stage.__swap_orders_20260101;

DROP TABLE stage.__swap_orders_20260101;

-- Final cleanup
DROP TABLE stage.__stage_orders;

ANALYZE marts.orders;
```

---

### 2. Heap table

> **Distribution:** if `distributed_by` is not set, the table is created with `DISTRIBUTED RANDOMLY`.
> `DISTRIBUTED REPLICATED` is not supported: partitioned tables in Greenplum 6 are incompatible with replicated distribution.

#### 2.1. Overwrite mode

```sql
{{
  config(
    schema='marts',
    materialized='incremental',
    incremental_strategy='exchange_partition',
    exchange_partition_key='event_date',
    exchange_stage_schema='stage',
    exchange_merge_partitions=false,
    exchange_partition_granularity='day',
    exchange_initial_partition_start_at='2026-01-01',
    distributed_by='id'
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

Generated SQL (first run + one partition period):
```sql
-- Create target (first run)
CREATE TABLE marts.orders (
  "id" bigint,
  "event_date" date,
  "user_id" integer,
  "amount" numeric(18,4),
  "loaded_at" timestamp
)
DISTRIBUTED BY (id)
PARTITION BY RANGE (event_date) (
    START (DATE '2026-01-01') INCLUSIVE
    END   (DATE '2026-01-02') EXCLUSIVE
    EVERY (INTERVAL '1 day')
);

DROP TABLE IF EXISTS stage.__stage_orders;

CREATE TABLE stage.__stage_orders (
  "id" bigint,
  "event_date" date,
  "user_id" integer,
  "amount" numeric(18,4),
  "loaded_at" timestamp
)
DISTRIBUTED BY (id);

INSERT INTO stage.__stage_orders (
    SELECT
        id,
        event_date::date AS event_date,
        user_id,
        amount,
        current_timestamp::timestamp AS loaded_at
    FROM orders
    WHERE event_date::date BETWEEN '2026-01-01'::date AND '2026-01-02'::date
);

CREATE TABLE stage.__swap_orders_20260101
AS
SELECT id, event_date, user_id, amount, loaded_at
FROM stage.__stage_orders
WHERE date_trunc('day', event_date::timestamptz) = DATE '2026-01-01'
DISTRIBUTED BY (id);

ALTER TABLE marts.orders
  EXCHANGE PARTITION FOR (DATE '2026-01-01')
  WITH TABLE stage.__swap_orders_20260101;

DROP TABLE stage.__swap_orders_20260101;
DROP TABLE stage.__stage_orders;
ANALYZE marts.orders;
```

---

#### 2.2. Merge mode

```sql
{{
  config(
    schema='marts',
    materialized='incremental',
    incremental_strategy='exchange_partition',
    exchange_partition_key='event_date',
    exchange_stage_schema='stage',
    exchange_merge_partitions=true,
    unique_key=['id'],
    exchange_partition_granularity='day',
    distributed_by='id'
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
DROP TABLE IF EXISTS stage.__stage_orders;

CREATE TABLE stage.__stage_orders (
  "id" bigint,
  "event_date" date,
  "user_id" integer,
  "amount" numeric(18,4),
  "loaded_at" timestamp
)
DISTRIBUTED BY (id);

INSERT INTO stage.__stage_orders (
    SELECT
        id,
        event_date::date AS event_date,
        user_id,
        amount,
        current_timestamp::timestamp AS loaded_at
    FROM orders
    WHERE updated_at BETWEEN '2026-01-01 00:00:00'::timestamp
                          AND '2026-01-02 00:00:00'::timestamp
);

CREATE TABLE stage.__swap_orders_20260101
AS
SELECT id, event_date, user_id, amount, loaded_at
FROM stage.__stage_orders
WHERE date_trunc('day', event_date::timestamptz) = DATE '2026-01-01'

UNION ALL

SELECT id, event_date, user_id, amount, loaded_at
FROM marts.orders __trg
WHERE date_trunc('day', __trg.event_date::timestamptz) = DATE '2026-01-01'
  AND NOT EXISTS (
      SELECT 1
      FROM stage.__stage_orders __delta
      WHERE date_trunc('day', __delta.event_date::timestamptz) = DATE '2026-01-01'
        AND __trg."id" = __delta."id"
  )
DISTRIBUTED BY (id);

ALTER TABLE marts.orders
  EXCHANGE PARTITION FOR (DATE '2026-01-01')
  WITH TABLE stage.__swap_orders_20260101;

DROP TABLE stage.__swap_orders_20260101;
DROP TABLE stage.__stage_orders;
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
