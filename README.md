# Sales Data Coding Challenge — Solution Overview

Evaluator-facing summary of this pipeline, describing exactly what is implemented in this
folder (these notebooks, `common/*.py`, the dashboards, and the Job definition).

Plain-markdown mirror of `README.py` (the Databricks notebook source), kept for easy
reading outside a Databricks workspace. `README.py` is the canonical version — re-generate
this file from it if it changes.

## 0. Repo layout & deploying this bundle

This repo *is* a [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/):

```
databricks.yml                          # bundle entry point: name, include:, dev/prod targets
resources/
  sales_data_cc_pipeline.job.yml        # the 4-task Job (setup → bronze → silver → gold)
  gold_task1_sales_by_city.dashboard.yml
  gold_task2_product_performance.dashboard.yml
src/
  00_setup.py, 01_bronze_ingestion.py, 02_silver_transform.py, 03_gold_aggregations.py
  common/{gold,silver,quality_checks}.py
  dashboards/*.lvdash.json              # the two Lakeview dashboards, as exported JSON
  adhoc/profiling_notebook.py           # standalone data-profiling notebook — NOT a job task
  adhoc/delta_processing_check.py       # standalone delta-load verification notebook — NOT a job task
```

The `dev` target in `databricks.yml` is bound to the live Job/dashboards already running in
the author's workspace (via `databricks bundle generate --bind`), so `bundle deploy -t dev`
updates them in place rather than creating duplicates.

**To deploy/run this yourself, from the repo root:**

```bash
databricks auth login --host <your-workspace-url>   # once, per machine
databricks bundle validate                           # sanity-check the YAML
databricks bundle deploy -t dev                      # upload src/ + create/update Job & dashboards
databricks bundle run sales_data_cc_pipeline          # trigger the 4-task Job
```

`bundle deploy` prints the workspace links for the deployed Job and both dashboards. The two
notebooks under `src/adhoc/` are deployed alongside the pipeline but are deliberately **not**
wired into the Job — open them directly in the workspace and run manually for extra
verification (profiling of the raw CSVs, and a Bronze→Silver→Gold check of a `load_mode=delta`
incremental run).

## 1. Tasks solved

| Task | Ask (original brief) | Where it's answered |
|---|---|---|
| **Task 1** | Total sales volume (accounting for discounts) for the **WOMAN** section, **winter** season, per **city** | `gold.task1_sales_facts` (sale-grain view) and `gold.task1_sales_rollup` (pre-aggregated by section/season/month/city) — filter to `section='WOMAN', season='WINTER'` and sum `effective_sales_volume` / `total_effective_sales_volume` by `city` |
| **Task 1b** | Compare Task 1's volume to a scenario where each city's **maximum available discount** is applied to all relevant products instead | Computed live in the **"Gold Task 1 — Sales by City"** Lakeview dashboard SQL (`MAX(max_discount)` over the current filtered rollup selection, re-priced volume via `SUM(gross_sales_volume) * (1 - city_max_discount)`) — not a persisted Gold table, so it always reflects the dashboard's current filters |
| **Task 2** | Identify best- vs. worst-selling products and look for patterns in attributes / sales month | 7 `gold.task2_*` tables built in `03_gold_aggregations.py` from `common/gold.py`, visualized in the **"Gold Task 2 — Product Performance"** dashboard |

Source data: `product_information.csv`, `products_sold.csv`, `discount.csv` (three flat
files, per the original brief).

## 2. Architecture

Medallion lakehouse on Unity Catalog + Delta. Tables follow `<catalog>.<layer>.<entity>`
(default catalog `sales_data_cc`, set by the `catalog` widget in `00_setup.py`).

| Layer (schema) | Contents | Built by |
|---|---|---|
| `landing` | Unity Catalog Volume `raw_files` (uploaded CSVs) + `checkpoints` Volume (Auto Loader state) | `00_setup.py` |
| `bronze` | `products_raw`, `sales_raw`, `discounts_raw` — string columns + lineage (`_ingested_at`, `_source_file`, `_ingestion_mode`, `_rescued_data`) | `01_bronze_ingestion.py` |
| `silver` | `products`, `discounts`, `sales`, `sales_enriched` — typed, normalized, deduplicated | `02_silver_transform.py` + `common/silver.py` |
| `quarantine` | `products`, `discounts`, `sales` — rejected rows with a `_dq_rule` label and lineage | `02_silver_transform.py` + `common/quality_checks.py` |
| `gold` | `task1_sales_facts` (view), `task1_sales_rollup`, 7 `task2_*` tables | `03_gold_aggregations.py` + `common/gold.py` |

**Notebook/module split:** the four numbered notebooks are thin orchestration; all
reusable casting, quarantine-rule, and aggregation logic lives in `common/silver.py`,
`common/quality_checks.py`, and `common/gold.py`, loaded via `%run`. **Why:** casting and
quarantine rules are each needed by more than one entity (products/discounts/sales) and
`02`/`03` both read the same Silver outputs, so factoring that logic out avoids duplicating
it across notebooks and keeps each notebook readable as pure orchestration. As plain
importable Python functions, `common/*.py` is also structured to be unit-testable outside
a Databricks runtime.

**Orchestration:** `sales_data_cc_pipeline.yaml` defines a Databricks Job with 4 tasks —
`setup → bronze → silver → gold` (linear `depends_on` chain), a **file-arrival trigger**
on the landing Volume, `max_retries: 1` per task, and parameters
`catalog=sales_data_cc`, `load_mode=delta` (default), `volume_name=raw_files`.

**Dashboards:** two Lakeview dashboards under this folder's `dashboards/` —
*Gold Task 1 — Sales by City* (built on `task1_sales_facts`/`task1_sales_rollup`) and
*Gold Task 2 — Product Performance* (built on the `task2_*` tables).

**Verification notebook:** `delta_processing_check.py` is a separate, standalone notebook
that proves a `load_mode=delta` run correctly picks up new files end to end — bronze →
silver → gold, including quarantine routing — using three deliberately-planted delta CSVs
with brand-new keys.

## 3. Implementation highlights

- **Bronze ingestion uses Auto Loader** (`cloudFiles`), one streaming source per entity,
  matched by filename prefix (`pathGlobFilter`) in the landing Volume, each with its own
  checkpoint/schema directory and run with `trigger(availableNow=True)` — new files are
  picked up incrementally and tracked via checkpoint state, not a manual filename guard.
  `load_mode=initial` wipes the Bronze table *and* its checkpoint/schema state for a clean
  rebuild; `delta` only processes files not yet seen by that checkpoint. Every source is
  attempted even if one fails, so partial success is visible — but the notebook raises
  `RuntimeError` if any source failed, failing the Job rather than silently continuing.
- **Silver casting is fail-soft, not fail-silent:** every cast uses `try_cast`, so a
  malformed value quarantines that row instead of aborting the batch; a fail-fast
  `verify_columns` check (`common/silver.py`) raises immediately if Bronze is missing an
  expected business column, instead of silently relying on schema merge.
- **Quarantine rules** (`common/quality_checks.py`): null business keys, unparseable/
  out-of-range values (price, discount `[0,1]`, non-positive `pieces_sold`), conflicting
  values under the same natural key, and — a dedicated rule — discounts/sales whose
  `product_id` has no match among *this run's* valid products are quarantined as
  `missing_product_reference`, keeping Silver's declared foreign keys true.
- **Natural keys & merge:** `products(product_id)`, `discounts(product_id, city)`,
  `sales/sales_enriched(product_id, city, sale_ts)`. `write_silver_table` overwrites on
  first creation, otherwise runs a `MERGE WITH SCHEMA EVOLUTION` upsert by natural key.
- **`sales_enriched`** joins valid sales to products/discounts and computes
  gross/net amounts; its row count is asserted equal to `sales` before writing — a
  mismatch raises rather than writing a corrupted enrichment.
- **Delta constraints re-declared every run** (`declare_silver_constraints`): RELY
  primary/foreign keys (`products_pk`, `sales_products_fk`, `discounts_products_fk`) for
  documentation/optimizer hints, plus real, Delta-enforced `NOT NULL` and `CHECK`
  constraints (`discounts_discount_range`, `sales_pieces_sold_positive`) that the engine
  actually validates. The function is idempotent — existing constraints are detected and
  skipped.
- **Task 1 is a view, not a table** (`task1_sales_facts`): one row per sale with
  `effective_sales_volume = pieces_sold * price * (1 - COALESCE(discount, 0))`, always
  reading Silver live. `task1_sales_rollup` pre-aggregates the same facts at
  `(section, season, sale_month, city)` grain, **liquid-clustered** on those columns, so
  dashboard slices scan a small table instead of ~1M sale rows; `max_discount` and
  `gross_sales_volume` per cell make Task 1b's max-discount scenario re-combinable without
  re-touching Silver.
- **Task 2 ranking:** `percent_rank()` over lifetime `pieces_sold` per product (0 = fewest,
  1 = most) splits products into **bottom 10%** (`rank <= 0.10`), **top 10%**
  (`rank >= 0.90`), and **middle 80%**. Ties share a percentile value and stay together on
  one side of a cut — deterministic, unlike `NTILE(10)`. Catalog products with zero sales
  are zero-filled and included in the bottom group rather than dropped.
- Gold is **always rebuilt in full from Silver** on every run (no incremental Gold logic);
  one superseded legacy table (`task2_attribute_patterns`) is dropped every run so Gold
  only ever shows the current table set.

## 4. Key decisions & assumptions

| Topic | Decision |
|---|---|
| "Sales volume" | Interpreted as a **monetary amount**: `pieces_sold * price * (1 - discount)` |
| Missing discount | Treated as **0**, not unknown — discounts are confirmed `[0, 1]` fractions, never auto-rescaled from percentages |
| Task 2 grouping | **Percentile deciles** (top/bottom 10%, `percent_rank`) instead of raw min/max — raw extremes were a degenerate, tie-heavy ~1% of the catalog |
| Task 2 price | **Catalog list price**, not the discounted price actually paid — city discounts aren't folded into Task 2 |
| Sales event key | `(product_id, city, sale_ts)` — an assumed key since no stronger identifier exists in `products_sold.csv`; exact duplicates are deduped, genuine conflicts are quarantined |
| Timestamps | `time` is Unix epoch seconds → `timestamp_seconds`; session timezone pinned to **UTC** in `00_setup.py` so derived dates/months are deterministic |
| Reference joins | Rely on **Spark's automatic broadcast** (~10MB threshold) for products/discounts against sales, based on observed data scale (~20K products / ~184K discounts / ~1M sales) — documented, not pinned with explicit `broadcast()` hints |
| Data window | Sales span 2025-01-01 to 2025-11-29 (**no December**) — any "winter month" or "all N months" reading in Task 2 reflects this 11-month window, not a calendar year |

## 5. Data quality — enforced in code, not a separate gate

There is no standalone DQ notebook or dedicated Job task; correctness is enforced at the
point of failure instead, split across two distinct mechanisms:

**1. Quarantine rules (`common/quality_checks.py`)** — row-level, applied *before* Silver
is written, during `02_silver_transform.py`. A failing row is moved to `quarantine.*` with
a `_dq_rule` reason and its Bronze lineage, never dropped silently:

| Rule | Checks |
|---|---|
| `null_key_rule` | Business key column(s) are null |
| `null_or_unparseable_rule` | Source value missing, or `try_cast` to the target type failed (price, discount, `pieces_sold`, timestamps) |
| `out_of_range_rule` | Cast `discount` outside `[0, 1]` |
| `non_positive_rule` | Cast `pieces_sold <= 0` |
| `conflicting_duplicate_rule` | Same natural key appears with differing business values (not an exact duplicate) |
| `split_missing_reference` | Discount/sale `product_id` has no match among *this run's* valid products — tagged `missing_product_reference` |

**2. Delta table constraints (`declare_silver_constraints` in `common/silver.py`)** —
table-level, re-declared idempotently on the already-written Silver tables every run. Two
different enforcement strengths, both present:

| Constraint | Kind | Enforced by the engine? |
|---|---|---|
| `products_pk` — `PRIMARY KEY (product_id)` | RELY | **No** — documentation/optimizer hint only, Delta/UC does not validate it |
| `sales_products_fk`, `discounts_products_fk` — `FOREIGN KEY (product_id) REFERENCES products` | RELY | **No** — same, relies on the quarantine layer above already having removed orphan keys |
| `NOT NULL` on `products.product_id`; `discounts.product_id/city/discount`; `sales.product_id/city/sale_ts/pieces_sold` | Enforced | **Yes** — write fails if violated |
| `discounts_discount_range` — `CHECK (discount BETWEEN 0 AND 1)` | Enforced | **Yes** |
| `sales_pieces_sold_positive` — `CHECK (pieces_sold > 0)` | Enforced | **Yes** |

So discount range, `pieces_sold` positivity, and null keys are checked **twice** by design:
once pre-write (quarantine rule, routes the row to `quarantine.*` with a reason) and once
post-write (Delta `CHECK`/`NOT NULL`, a hard engine-level guarantee on the table itself,
independent of whether the quarantine logic was bypassed or has a bug). The PK/FK pair is
RELY-only and depends entirely on `split_missing_reference` already having removed orphan
keys — it is not a second enforcement layer, just a documented/optimizer-visible guarantee.

**Other checks, outside both of the above:**

- **Fail-fast schema contract:** `verify_columns` raises immediately if Bronze is missing
  an expected business column, instead of letting `mergeSchema` silently absorb drift.
- **Row-count assertion** on `sales_enriched` vs. `sales` raises before writing if a join
  fanned out or dropped rows.
- **Bronze ingestion raises `RuntimeError`** if any source fails, so the Job stops instead
  of reporting false success.
- `02_silver_transform.py` also prints informational **WARN**-level counts (quarantine
  volume by rule, sales with no matching discount, products never sold) — these never fail
  the run, they're observability only.

Net effect: a bad run fails loudly (or quarantines quietly, by design) at the exact step
that detects the problem, rather than flowing through to Gold and requiring a separate
downstream check to catch it.

## 6. Key findings (from the actual data, via the dashboards / profiling)

- `products_sold.csv` / `discount.csv` contain **orphan `product_id`s** with no match in
  `product_information.csv` — handled as a standing `missing_product_reference` quarantine
  rule, not a one-off cleanup.
- Discounts **cap out at exactly 20%**, with a large share of product/city discount rows
  sitting at that ceiling (dashboard: *"Discounts set at exactly 20% — the deepest possible
  discount"*).
- Daily sales volume is **flat across the 11-month window — no detectable seasonality**
  (dashboard: *"Pieces per day by month – flat rate, no seasonality"*).
- Among Task 2 attributes, **Origin** shows a measurable share delta between top-10% and
  bottom-10% sellers, while **Promotion is a weak separator** — both are kept and labeled
  as findings (a non-signal is still a finding, not noise).
- **Activity breadth** (number of distinct months a product sold in) **closely tracks the
  decile cut** — top-decile products tend to have sold across most/all 11 months,
  bottom-decile products in few or none — so the attribute deltas partly reflect
  consistency, not pure attribute appeal.
- No calendar month separates the top-10%/bottom-10% mix when each group's pieces are
  compared as a share of its own total (`task2_month_share_deltas`).

## 7. Delta-load verification (`delta_processing_check.py`)

**What it introduces:** a small delta batch of three CSVs (5/8/10 rows) layered on top of
the original full load for a `load_mode=delta` run, using **brand-new keys only**
(`900001`–`900003`, `700001`–`700003`, `999999`) plus one new sale on an **existing** key
(`185102`) — so merging never touches an already-loaded row. The clean rows introduce
dimension values absent from the original data (brand `H&M`, category `electronics`,
section `KIDS`, season `Monsoon`, city `Austin`) and four discounts `> 20%`, so they're
easy to spot as new. The broken rows are deliberately isolated on their own new keys: a
null/unparseable price, an out-of-range discount (`1.50`), a negative `pieces_sold`, and
three conflicting-duplicate key pairs (one each in products/discounts/sales).

**What `delta_processing_check.py` asserts:** Bronze ingests all 23 delta rows exactly
once (no re-ingestion of the initial files); Silver merges the 14 clean rows by key (2
products, 5 discounts, 7 sales) while the 9 broken rows land in quarantine tagged with the
correct `_dq_rule` and never leak into Silver; the new dimension values show up as new
distinct values in `silver.products`/`silver.discounts`/`silver.sales`; and
`gold.task1_sales_facts`/`task1_sales_rollup` reflect the new keys with correct
`effective_sales_volume` arithmetic.

**Cross-check against current code:** the recorded run shows sales on keys with no
matching product (`999999`, `700002`, and `900003`'s sale) merged into `silver.sales` and
only excluded later at the Gold view's `INNER JOIN` — not quarantined in Silver. That
predates the `missing_product_reference` rule now present in
`common/quality_checks.py`/`common/silver.py`; re-running this check today would likely
quarantine those three rows in Silver instead. The check's own follow-up note is still
relevant and verified: because the silver `sales_products_fk` is RELY-only, the optimizer
can drop the products join and over-count orphan keys in `COUNT(*)` queries through the
fact view, even though row-level `show`/`collect` results stay correct.

## 8. Scope: challenge implementation vs. a real production project

Several choices below pick between genuinely different architectural options. For a
time-boxed challenge on three flat CSVs, the simpler/cheaper option was taken deliberately;
a larger real project would often weigh it differently. Flagged explicitly so the choice
reads as a scoping decision, not an oversight:

| Decision point | Chosen here (challenge scope) | What a real project would likely weigh instead |
|---|---|---|
| Bronze ingestion mechanism | **Auto Loader** (`cloudFiles`) over CSVs dropped in a Volume | If the source were a true event stream (order system, POS), **Kafka/Event Hubs** feeding Structured Streaming  — Auto Loader is for files, not for replacing a message bus |
| Data quality enforcement | Hand-written quarantine rules (`common/quality_checks.py`) + manually re-declared Delta `CHECK`/`NOT NULL`/RELY constraints | **Lakeflow Declarative Pipelines (DLT)** `expect_or_drop`/`expect_or_fail` expectations, or a dedicated framework (e.g. Great Expectations/Deequ) for a shared, reusable rule catalog with built-in metrics — more setup cost, less hand-rolled plumbing at larger team/table scale |
| Task 1 result shape | A plain **view** (`task1_sales_facts`) for row-level truth + a manually built/refreshed **rollup table** for pre-aggregation | Databricks **Materialized Views** (via Lakeflow) would give managed incremental refresh instead of a full rebuild-from-Silver on every Gold run — trades manual control for less code to maintain |
| Reference joins (products/discounts into sales) | Left to **Spark's automatic broadcast** at the current ~20K/~184K row scale | At materially larger or less predictable reference-table sizes, a real project would pin explicit `broadcast()` hints and add a scale regression check rather than rely on the default threshold holding |
| Orchestration | One linear 4-task Databricks Job (`setup → bronze → silver → gold`), hand-authored YAML | A larger project would likely add conditional/retry branching, failure alerting, and environment promotion (dev/stage/prod) rather than a single hard-coded Job |
| Compute | Serverless, Databricks Free Edition | A production workload would size/pin job clusters (autoscaling policy, instance types, cost controls) rather than rely on serverless defaults |
| Local test coverage | None for the current `common/*.py` logic | A real project would have CI-gated unit tests per `common/*.py` function plus integration tests against a dev catalog before merge |

Each ties back to the rationale already given in the sections above; this table is the
one-page lens for reading them as scoping calls rather than gaps.

## 9. Known gaps

- No alerting/notifications wired to the Job (failures are visible in Workflows UI only).
- No automated test coverage for the current `common/*.py` logic.
- Auto-broadcast join sizing is a documented assumption based on observed data scale, not
  load-tested against significantly larger inputs.