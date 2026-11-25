# Sales Data Coding Challenge — Solution

A Databricks Asset Bundle (DAB) implementing a medallion (bronze/silver/gold) pipeline on
Unity Catalog + Delta for the sales-data coding challenge, with two Lakeview dashboards and
Databricks Jobs for orchestration.

- **To deploy and run it:** see **`HOW-TO-RUN.md`** (Databricks UI only, no CLI needed).

## Tasks solved

| Task | Ask | Where |
|---|---|---|
| 1 | Sales volume (after discounts) for **WOMAN** section, **winter** season, per city | `gold.task1_sales_facts` / `task1_sales_rollup` |
| 1b | Same, if each city's max available discount applied to all products | Computed live in the Task 1 dashboard SQL |
| 2 | Best/worst-selling products, patterns in attributes/month | 7 `gold.task2_*` tables, Task 2 dashboard |

Source data: `product_information.csv`, `products_sold.csv`, `discount.csv`.

## Repo structure

```
databricks.yml                  # bundle definition, single `dev` target
resources/
  sales_data_cc_pipeline.job.yml  # bronze → silver → gold, file-arrival triggered
  sales_data_cc_seed.job.yml      # manual-only: seeds the landing Volume (simulates a file drop)
src/
  common/config.py              # shared widgets, catalog/schema/volume DDL, path helpers
  common/silver.py              # typed casting, Silver writer, constraint declarations
  common/quality_checks.py      # quarantine rules
  common/gold.py                # Gold aggregation logic
  seed_landing_volume.py        # sales_data_cc_seed's only task
  01_bronze_ingestion.py        # Auto Loader, one stream per entity
  02_silver_transform.py        # cast, quarantine, merge upsert
  03_gold_aggregations.py       # full rebuild of Gold from Silver
  dashboards/                   # two Lakeview dashboard JSON definitions
  adhoc/
    profiling_notebook.py       # standalone raw-CSV profiling (always the initial CSVs only)
    delta_processing_check.py   # standalone end-to-end delta-load verification
data/
  initial/*.csv                 # full starting dataset, seeded on every first run
  delta/*.csv                   # small incremental batch, seeded only when requested
```

## Architecture

Medallion lakehouse: `<catalog>.<layer>.<entity>` (default catalog `sales_data_cc`).

| Layer | Contents | Built by |
|---|---|---|
| `landing` | Volume with seeded CSVs + Auto Loader checkpoints | `seed_landing_volume.py` |
| `bronze` | Raw string columns + ingestion lineage | `01_bronze_ingestion.py` (Auto Loader) |
| `silver` | Typed, deduplicated, merged by natural key | `02_silver_transform.py` |
| `quarantine` | Rejected rows, tagged with a `_dq_rule` reason | `02_silver_transform.py` |
| `gold` | Task 1 view + rollup, 7 Task 2 tables | `03_gold_aggregations.py` |

**Orchestration — two Jobs:**
- **`sales_data_cc_pipeline`** — the real pipeline (`bronze → silver → gold`), triggered
  automatically by a file-arrival trigger on the landing Volume.
- **`sales_data_cc_seed`** — manual-only, simulates an external system dropping CSVs into
  the landing Volume (standing in for whatever real upstream would do this in production).
  Kept as a separate, trigger-less Job so it can write to the Volume without the pipeline's
  own trigger ever reacting to its own output.

## Implementation highlights

- **Auto Loader** (`cloudFiles`), one stream per entity, incremental via checkpoint state;
  `load_mode=initial` resets Bronze + checkpoints for a clean rebuild, `delta` only
  processes unseen files.
- **Fail-soft casting, fail-fast schema:** every Silver cast uses `try_cast` (bad values
  quarantine the row, not the batch); a missing expected Bronze column raises immediately.
- **Quarantine rules:** null keys, unparseable/out-of-range values, conflicting duplicates
  under the same key, and orphan `product_id` references (`missing_product_reference`).
- **Delta constraints re-declared every run:** RELY PK/FK for documentation/optimizer
  hints, plus real engine-enforced `NOT NULL`/`CHECK` constraints.
- **Task 1** is a live view (`task1_sales_facts`) plus a liquid-clustered rollup table for
  fast dashboard slicing; Task 1b's max-discount scenario is computed live in the dashboard
  from the rollup, not a persisted table.
- **Task 2 ranking** uses `percent_rank()` deciles (top/bottom 10%), not raw min/max —
  deterministic under ties, and zero-sales catalog products are included, not dropped.
- Gold is always rebuilt in full from Silver on every run (no incremental Gold logic).

## Key decisions & assumptions

| Topic | Decision |
|---|---|
| "Sales volume" | `pieces_sold * price * (1 - discount)`, a monetary amount |
| Missing discount | Treated as 0 |
| Task 2 grouping | Percentile deciles, not raw extremes (which were tie-heavy/degenerate) |
| Task 2 price | Catalog list price, not discounted price paid |
| Sales event key | `(product_id, city, sale_ts)` — no stronger identifier exists in the source |
| Timestamps | Unix epoch seconds → UTC timestamps (session timezone pinned to UTC) |
| Reference joins | Rely on Spark's automatic broadcast at current data scale (~20K/~184K/~1M rows) |
| Data window | Source spans 2025-01-01 to 2025-11-29 (no December) |

## Data quality — enforced in code, not a separate gate

No standalone DQ notebook or Job task — correctness is enforced where it's detected:
row-level quarantine rules before Silver is written, plus Delta table constraints
re-declared after.

## Key findings from the data

- `products_sold.csv`/`discount.csv` contain orphan `product_id`s with no match in
  `product_information.csv` — handled by a standing quarantine rule.
- Discounts cap out at exactly 20%.
- Daily sales volume is flat across the 11-month window — no detectable seasonality.
- Among Task 2 attributes, **Origin** separates top/bottom deciles; **Promotion** is a weak
  separator. Activity breadth (distinct months sold) closely tracks the decile cut.
- No calendar month separates the top/bottom mix once each group is compared as a share of
  its own total.
- `pieces_sold` is skewed high (range 1–99, median 70) — unlike typical real retail where
  low quantities dominate, a likely synthetic-data signature worth remembering when
  interpreting volume metrics.

## Delta-load verification

`src/adhoc/delta_processing_check.py` proves a `load_mode=delta` run merges new rows into
Silver correctly and routes bad ones to `quarantine.*`, end to end through Gold. See
`HOW-TO-RUN.md` for how to trigger a delta run from the UI.

## Scope: challenge vs. a real production project

A few choices below trade simplicity for speed, appropriate for a time-boxed challenge on
three flat CSVs; a larger real project would weigh them differently:

| Decision point | Chosen here | A real project would likely use |
|---|---|---|
| Bronze ingestion | Auto Loader over CSVs in a Volume | Kafka/Event Hubs if the source were a true event stream |
| Data quality | Hand-written quarantine rules + Delta constraints | Lakeflow Declarative Pipelines expectations |
| Task 1 result | View + manually rebuilt rollup table | Databricks Materialized Views for managed incremental refresh |
| Reference joins | Default Spark auto-broadcast | Explicit `broadcast()` hints + a scale regression check |
| Orchestration | Two Jobs, flat `max_retries: 1`, one `dev` target | Conditional branching, failure alerting, dev/staging/prod promotion |
| Compute | Serverless, Databricks Free Edition | Sized/pinned job clusters with cost controls |
| Tests | None | CI-gated unit + integration tests |

## Known gaps

- No alerting/notifications wired to the Job (failures visible in Workflows UI only).
- No automated test coverage for `common/*.py`.
- Auto-broadcast join sizing is a documented assumption, not load-tested at larger scale.
