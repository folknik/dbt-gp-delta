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
 - 0.1.0

### `exchange_partition` incremental strategy

The `exchange_partition` strategy implements atomic, partition-level incremental loads using Greenplum's `EXCHANGE PARTITION` DDL operation. Instead of inserting rows into the live table, it builds a separate swap table for each affected partition period and atomically replaces the partition via a metadata-only operation — no physical data movement occurs.

#### How it works

1. On the **first run**, dbt creates the target table as a range-partitioned table using `exchange_partition_key` as the partition column. The initial partition range is controlled by `exchange_initial_partition_start_at` / `exchange_initial_partition_end_at`.
2. On every **incremental run**:
   - The model SQL is materialised into a staging table in `exchange_stage_schema`.
   - The strategy inspects the staging data to find the distinct set of partition periods that need to be updated (`DISTINCT date_trunc(..., partition_key)`).
   - For each period:
     - If the partition does not yet exist it is created automatically (`ALTER TABLE ... ADD PARTITION`).
     - A swap table is built in `exchange_stage_schema` with the same storage options and distribution policy as the target:
       - **overwrite mode** (`exchange_merge_partitions=false`): swap = staging rows for this period only.
       - **merge mode** (`exchange_merge_partitions=true`): swap = staging rows UNION ALL existing partition rows that are **not** matched by `unique_key` (new rows win over old ones).
     - `ALTER TABLE target EXCHANGE PARTITION FOR (DATE '...') WITH TABLE swap` atomically replaces the partition.
     - The swap table is dropped.
   - The staging table is dropped.
   - `ANALYZE` is run on the target table (unless disabled).

#### Required model config

| Parameter | Type | Description |
|---|---|---|
| `exchange_partition_key` | string | Column used as the partition key. Must be castable to `timestamptz`. |
| `exchange_stage_schema` | string | Schema where staging and swap tables are created. Must exist in the database. |
| `exchange_merge_partitions` | bool | `true` — merge new rows with existing partition data (requires `unique_key`). `false` — fully overwrite affected partitions with staging data. |
| `contract: enforced: true` | — | Column definitions must be declared in `schema.yml`. Required because the table is created with an explicit DDL rather than CTAS. |

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
- Swap tables are named `__swap_{model_name}_{YYYYMMDD}` (day) or `__swap_{model_name}_{YYYYMM}` (month) and are always dropped after a successful exchange. If a run is interrupted they will be cleaned up on the next run.
- Staging tables are named `__stage_{model_name}` and live in `exchange_stage_schema`. They are also dropped after a successful run.
- `WITHOUT VALIDATION` (`exchange_allow_with_validation=false`) skips Greenplum's constraint check during the exchange. Use it only when you are certain the swap table data satisfies the partition constraints.
- The strategy processes only partition periods that are **actually present in the staging data**. Periods with no new data are never touched, so existing partition data for those periods is preserved as-is.

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
