# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,03 — Gold Aggregations
# MAGIC %md
# MAGIC # 03 — Gold Aggregations
# MAGIC Publishes every gold object as a Databricks **materialized view** (MV): Task 1's
# MAGIC sale-grain facts (`sales_data_cc.gold.task1_sales_facts`, the single gold home of the
# MAGIC sales-products-discounts join) and its pre-aggregated (section, season, sale_month,
# MAGIC city) rollup, the discount-terms and filter-options lookups that back the dashboard's
# MAGIC filter widgets, and Task 2 (top 10% vs bottom 10% product groups, attribute deltas,
# MAGIC monthly patterns, and sales-timing analyses). Each object's `SELECT` reads only
# MAGIC silver or an earlier gold MV in this same chain; every dashboard dataset and every
# MAGIC downstream gold object reads gold only.
# MAGIC
# MAGIC `ensure_materialized_view()` creates each MV the first time it's built, then only
# MAGIC refreshes it on every later run, in dependency order. This runs synchronously inside
# MAGIC the existing bronze → silver → gold job, so a file-arrival-triggered run always
# MAGIC leaves every MV reflecting that run's silver data.

# COMMAND ----------

# MAGIC %run ./common/config

# COMMAND ----------

# MAGIC %run ./common/gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver inputs
# MAGIC Full-data sizing context: ~20K products / ~184K silver discounts / ~1M sales. No
# MAGIC explicit broadcast hints: every reference join side is well under Spark's default
# MAGIC ~10 MB auto-broadcast threshold, so Spark broadcasts them automatically. If silver
# MAGIC grows substantially, re-verify sizes before assuming automatic broadcast applies.

# COMMAND ----------

silver_products = table_name("silver", "products")
silver_sales = table_name("silver", "sales")
silver_discounts = table_name("silver", "discounts")

# COMMAND ----------

# DBTITLE 1,Task 1 — sale-grain facts, rollup, discount terms, filter options
# MAGIC %md
# MAGIC ## Task 1 — sale-grain facts, rollup, discount terms, filter options
# MAGIC `sales_data_cc.gold.task1_sales_facts` is an MV with one row per sale: city, product_id,
# MAGIC pieces_sold, price, discount, section, season, sale_month, sale_ts, and the
# MAGIC effective_sales_volume business metric (`pieces_sold * price * (1 - discount)`). The
# MAGIC sales-products-discounts join lives in gold once; this MV is the semantic source of
# MAGIC truth and the build input for every other Task 1 object below.
# MAGIC
# MAGIC `sales_data_cc.gold.task1_sales_rollup` pre-aggregates the same facts at
# MAGIC (section, season, sale_month, city) grain so heavy dashboard slices read a small MV
# MAGIC instead of ~1M sale rows: additive pieces/volume metrics plus `gross_sales_volume` and
# MAGIC `max_discount` per grain, liquid-clustered on the dashboard's filter keys plus `city`
# MAGIC (the dashboard's city-sales page always groups by city, so it is part of the
# MAGIC clustering key, not an intentional omission). Any selection's city-max discount is
# MAGIC `MAX(max_discount)` over the selected rollup rows, and its max-discount volume is
# MAGIC `SUM(gross_sales_volume) * (1 - city_max_discount)` — both re-combine exactly.
# MAGIC `distinct_product_count` is exact only at this grain — for exact per-city distinct
# MAGIC counts, use the fact MV.
# MAGIC
# MAGIC `sales_data_cc.gold.task1_discount_terms` and `sales_data_cc.gold.task1_filter_options`
# MAGIC back the discount-distribution widgets and the section/season/sale-month filter
# MAGIC option lists, so every Task 1 dashboard dataset reads gold only.

# COMMAND ----------

# DBTITLE 1,Task 1 — sale-grain facts
task1_sales_facts = table_name("gold", "task1_sales_facts")
ensure_materialized_view(
    task1_sales_facts,
    task1_sales_facts_sql(silver_sales, silver_products, silver_discounts),
    comment=(
        "Task 1: one row per sale — city, product_id, pieces_sold, price, actual "
        "discount (0 when none applies), section, season, sale_month, sale_ts, and "
        "effective_sales_volume = pieces_sold * price * (1 - discount). The single "
        "gold home of the sales-products-discounts join; every other Task 1 gold "
        "object and dashboard dataset reads this MV for that data. "
        "Materialized view, refreshed on every gold run."
    ),
)

# COMMAND ----------

# DBTITLE 1,Task 1 — rollup by section, season, sale month, city
task1_sales_rollup = table_name("gold", "task1_sales_rollup")
ensure_materialized_view(
    task1_sales_rollup,
    task1_sales_rollup_sql(task1_sales_facts),
    cluster_by=("section", "season", "sale_month", "city"),
    comment=(
        "Task 1: rollup of task1_sales_facts at (section, season, sale_month, city) "
        "grain with additive pieces/volume metrics, gross_sales_volume, and the max "
        "discount within each grain cell. Any selection's city-max discount is "
        "MAX(max_discount) over the selected rollup rows and its max-discount volume "
        "is SUM(gross_sales_volume) * (1 - city_max_discount); distinct_product_count "
        "is exact only at this grain (use the fact MV for per-city distinct counts). "
        "Materialized view, refreshed on every gold run."
    ),
)

# COMMAND ----------

# DBTITLE 1,Task 1 — discount terms
task1_discount_terms = table_name("gold", "task1_discount_terms")
ensure_materialized_view(
    task1_discount_terms,
    task1_discount_terms_sql(silver_discounts, silver_products, silver_sales),
    comment=(
        "Task 1: one row per (product_id, city, sale_month-or-NULL) discount term — "
        "section, season, raw discount, 1%-granularity discount_pct, and the sale "
        "month it was sold in (NULL when that product/city pair never sold). Backs "
        "both Task 1 discount-distribution dashboard widgets. "
        "Materialized view, refreshed on every gold run."
    ),
)

# COMMAND ----------

# DBTITLE 1,Task 1 — filter options
task1_filter_options = table_name("gold", "task1_filter_options")
ensure_materialized_view(
    task1_filter_options,
    task1_filter_options_sql(silver_products, silver_sales),
    comment=(
        "Task 1: long/narrow (filter_type, filter_value) lookup of the distinct "
        "section, season, and sale_month values, backing the dashboard's filter "
        "widgets. Materialized view, refreshed on every gold run."
    ),
)

# COMMAND ----------

# DBTITLE 1,Task 2 — top 10% vs bottom 10% product groups
# MAGIC %md
# MAGIC ## Task 2 — top 10% vs bottom 10% product groups
# MAGIC Products are ranked by raw lifetime pieces sold across all cities and dates, then split
# MAGIC into percentile groups with `PERCENT_RANK()` (0 = fewest pieces, 1 = most pieces):
# MAGIC
# MAGIC - **top 10%** — `percentile_rank >= 0.90`
# MAGIC - **middle 80%** — the comparison base between the two cut-offs
# MAGIC - **bottom 10%** — `percentile_rank <= 0.10` (includes any unsold catalog products)
# MAGIC
# MAGIC Why percentile groups instead of raw min/max extremes: the extremes are
# MAGIC exact ties (one large tie at the maximum, one at the minimum) — degenerate ~1% slices
# MAGIC of the catalog. Deciles are statistically meaningful cohorts (~2,000 products each), and
# MAGIC tied products share a percentile value, so a tie spanning a cut-off pulls the whole tie
# MAGIC into one group: deterministic, unlike `NTILE(10)`, which would split boundary ties
# MAGIC arbitrarily. Compare the groups as **mixes** (how the top-decile population differs
# MAGIC from the bottom-decile population), not as identities of individual products. No
# MAGIC city/discount analysis is included in Task 2 at this stage.
# MAGIC
# MAGIC The seven MVs below answer, in order:
# MAGIC 1. `task2_performance_summary` — how big are the groups and does price differ between them?
# MAGIC 2. `task2_product_performance` — per-product pieces sold, with its percentile rank and group.
# MAGIC 3. `task2_top_10_vs_bottom_10_attribute_deltas` — which attribute values (position,
# MAGIC    season, material, terms, promotion, origin) separate the top 10% from the bottom 10%,
# MAGIC    measured as within-group share deltas in percentage points?
# MAGIC 4. `task2_monthly_patterns` — monthly pieces by calendar sale month and catalog season
# MAGIC    per group.
# MAGIC 5. `task2_sales_timing_summary` — how consistently does each group sell across months
# MAGIC    (active months, share selling in all vs a single sale month, pieces per active
# MAGIC    month, in-season share)?
# MAGIC 6. `task2_month_share_deltas` — do any calendar months separate the top 10% from the
# MAGIC    bottom 10% (within-group monthly share delta)?
# MAGIC 7. `task2_activity_breadth_by_group` — how closely does the decile cut follow activity
# MAGIC    breadth (months with sales)?
# MAGIC
# MAGIC **Catalog list price caveat:** the price comparison uses the listed product price.
# MAGIC Actual discounts can vary by product and city; they are not incorporated in this Task 2
# MAGIC analysis, so these metrics are not the price customers paid or realized revenue.

# COMMAND ----------

# DBTITLE 1,MV — task2_product_performance
# MAGIC %md
# MAGIC #### MV: `sales_data_cc.gold.task2_product_performance`
# MAGIC
# MAGIC Task 2: per-product total pieces sold across all cities and dates, with a
# MAGIC `percentile_rank` (0 = fewest pieces, 1 = most pieces) and a `performance_group`
# MAGIC label: `top 10%`, `middle 80%`, or `bottom 10%`. Unsold products are included with
# MAGIC zero pieces and fall in the bottom group. Materialized view, refreshed on every gold
# MAGIC run.

# COMMAND ----------

task2_product_performance = table_name("gold", "task2_product_performance")
ensure_materialized_view(
    task2_product_performance,
    task2_product_performance_sql(silver_sales, silver_products),
    comment=(
        "Task 2: per-product total pieces sold across all cities and dates, with a "
        "percentile_rank (0 = fewest pieces, 1 = most pieces) and a top 10% / middle "
        "80% / bottom 10% performance group from percent_rank over lifetime pieces. "
        "Ties share a percentile and stay in one group; unsold products are included "
        "with zero pieces and fall in the bottom group. Materialized view, refreshed "
        "on every gold run."
    ),
)

# COMMAND ----------

# DBTITLE 1,MV — task2_performance_summary
# MAGIC %md
# MAGIC #### MV: `sales_data_cc.gold.task2_performance_summary`
# MAGIC
# MAGIC Task 2: product counts, pieces-sold statistics, and catalog list-price statistics by
# MAGIC performance group (top 10% / middle 80% / bottom 10% by lifetime pieces sold). List
# MAGIC prices exclude discounts, so they are not realized revenue. Materialized view,
# MAGIC refreshed on every gold run.
# MAGIC
# MAGIC **What to observe** (phrased by column name so it stays true across rebuilds):
# MAGIC
# MAGIC - `product_count`: each decile holds roughly 10% of the catalog; ties at the cut-offs
# MAGIC   can make a group slightly larger. The three sizes always sum to the full catalog.
# MAGIC - `minimum_pieces_per_product` vs `maximum_pieces_per_product`: the top group's
# MAGIC   minimum sits above the bottom group's maximum — the groups never overlap.
# MAGIC - `average_catalog_list_price` / `median_catalog_list_price`: if these are similar
# MAGIC   across the groups, attribute mix and timing — not price — drive performance.

# COMMAND ----------

task2_performance_summary = table_name("gold", "task2_performance_summary")
ensure_materialized_view(
    task2_performance_summary,
    task2_performance_summary_sql(task2_product_performance),
    comment=(
        "Task 2: product counts, pieces-sold statistics, and catalog list-price "
        "statistics by performance group. List prices exclude discounts, so they are "
        "not realized revenue. Materialized view, refreshed on every gold run."
    ),
)

# COMMAND ----------

# DBTITLE 1,MV — task2_top_10_vs_bottom_10_attribute_deltas
# MAGIC %md
# MAGIC #### MV: `sales_data_cc.gold.task2_top_10_vs_bottom_10_attribute_deltas`
# MAGIC
# MAGIC Task 2: for every varying categorical product attribute value, the product counts and
# MAGIC within-group shares for the top 10% and bottom 10% seller groups, plus the share
# MAGIC delta. `top_10_percent_share_of_group` and `bottom_10_percent_share_of_group` are
# MAGIC fractions of the respective group (0-1); `share_delta_percentage_points` = (top share
# MAGIC minus bottom share) x 100, so +3.2 means the value is 3.2 percentage points more
# MAGIC common among top sellers than bottom sellers; `skews_towards` names the group with the
# MAGIC larger share. Materialized view, refreshed on every gold run.
# MAGIC
# MAGIC All attribute values are stored, including weak separators — "no signal" (e.g.
# MAGIC promotion) is a finding, not noise. Presentation layers select strong patterns with
# MAGIC `ABS(share_delta_percentage_points) >= 1` ("patterns above 1 percentage point").
# MAGIC
# MAGIC **What to observe (rows are ordered by the magnitude of `share_delta_percentage_points`):**
# MAGIC - The largest absolute deltas are the attribute values that separate the deciles;
# MAGIC   values near zero (e.g. promotion) do not separate them.
# MAGIC - Constant attributes (brand, product category, currency) are excluded from this
# MAGIC   MV: they cannot discriminate, so every group would show ~100% for them.
# MAGIC - A value that never appears in one group is stored with a genuine 0% share of that
# MAGIC   group (`top_10_percent_product_count` / `bottom_10_percent_product_count` = 0).

# COMMAND ----------

task2_attribute_deltas = table_name("gold", "task2_top_10_vs_bottom_10_attribute_deltas")
ensure_materialized_view(
    task2_attribute_deltas,
    task2_attribute_deltas_sql(task2_product_performance),
    comment=(
        "Task 2: within-group attribute shares for the top 10% vs bottom 10% seller "
        "groups, with the share delta in percentage points (top share minus bottom "
        "share; positive = over-represented among top sellers). All attribute values "
        "are kept, weak separators included; filter ABS(share_delta_percentage_points) "
        ">= 1 for strong patterns. Materialized view, refreshed on every gold run."
    ),
)

# COMMAND ----------

# DBTITLE 1,MV — task2_monthly_patterns
# MAGIC %md
# MAGIC #### MV: `sales_data_cc.gold.task2_monthly_patterns`
# MAGIC
# MAGIC Task 2: monthly pieces sold and within-group share by calendar sale month, product
# MAGIC catalog season, and performance group (top 10% / middle 80% / bottom 10% by lifetime
# MAGIC pieces sold). Materialized view, refreshed on every gold run.
# MAGIC
# MAGIC **What to observe:**
# MAGIC - Read `sale_month` (calendar month of the sale) and `season` (the product's catalog
# MAGIC   season) separately — Winter-catalog products also sell in summer months.
# MAGIC - Both deciles carry full-year volume, so top-vs-bottom timing patterns are directly
# MAGIC   comparable (unlike the old 201-product worst-tie group, which was too small for a
# MAGIC   meaningful monthly pattern).
# MAGIC - Compare `group_season_pieces_pct` across months within a season×group to see whether
# MAGIC   sales are spread evenly over the data window or spike in specific months.
# MAGIC - Data caveat: sales span 2025-01-01 through 2025-11-29, so December is absent from
# MAGIC   the data; any winter-month reading covers January and February only.

# COMMAND ----------

task2_monthly_patterns = table_name("gold", "task2_monthly_patterns")
ensure_materialized_view(
    task2_monthly_patterns,
    task2_monthly_patterns_sql(silver_sales, task2_product_performance),
    comment=(
        "Task 2: monthly pieces sold and within-group share by calendar sale month, "
        "product catalog season, and performance group (top 10% / middle 80% / "
        "bottom 10% by lifetime pieces sold). Materialized view, refreshed on every "
        "gold run."
    ),
)

# COMMAND ----------

# DBTITLE 1,Sales-timing analyses
# MAGIC %md
# MAGIC ### Sales-timing analyses: consistency, monthly deltas, activity breadth
# MAGIC
# MAGIC Three MVs extend the sales-month analysis beyond absolute volumes, which the
# MAGIC middle 80%'s scale would otherwise drown out:
# MAGIC
# MAGIC 1. `task2_sales_timing_summary` — per group: how many calendar months its products
# MAGIC    sell in (`average_active_sale_months_per_product`), the share of products selling
# MAGIC    in a single month vs in all of the data window's sale months (the month count is
# MAGIC    re-derived from the data on every refresh — currently 11, no December), average
# MAGIC    pieces per active month (intensity vs breadth), and the share of the group's
# MAGIC    pieces sold in calendar months matching the product's catalog season.
# MAGIC 2. `task2_month_share_deltas` — per calendar month, the top 10%'s share of its own
# MAGIC    pieces vs the bottom 10%'s share of its own, with the delta in percentage points.
# MAGIC    A near-zero delta is the finding that no month separates the deciles. A thin MV
# MAGIC    over `task2_monthly_patterns` only.
# MAGIC 3. `task2_activity_breadth_by_group` — months-with-sales tiers per group. The decile
# MAGIC    cut largely follows activity breadth: products with few active months concentrate
# MAGIC    in the bottom 10%, so the attribute deltas above read as combined
# MAGIC    appeal-and-consistency findings rather than pure attribute effects.

# COMMAND ----------

task2_sales_timing_summary = table_name("gold", "task2_sales_timing_summary")
ensure_materialized_view(
    task2_sales_timing_summary,
    task2_sales_timing_summary_sql(
        silver_sales, task2_product_performance, task2_monthly_patterns
    ),
    comment=(
        "Task 2: per-group sales-timing metrics — average active sale months per "
        "product, share of products selling in a single month vs in all of the "
        "data window's sale months (the count is re-derived on every refresh), "
        "average pieces sold per active month, and the share of the group's pieces "
        "sold in calendar months matching the product's catalog season. Materialized "
        "view, refreshed on every gold run."
    ),
)

# COMMAND ----------

task2_month_share_deltas = table_name("gold", "task2_month_share_deltas")
ensure_materialized_view(
    task2_month_share_deltas,
    task2_month_share_deltas_sql(task2_monthly_patterns),
    comment=(
        "Task 2: per calendar sale month, the top 10% and bottom 10% shares of "
        "their own lifetime pieces, the share delta in percentage points (top minus "
        "bottom), and which group the month skews towards. Materialized view, "
        "refreshed on every gold run."
    ),
)

# COMMAND ----------

task2_activity_breadth = table_name("gold", "task2_activity_breadth_by_group")
ensure_materialized_view(
    task2_activity_breadth,
    task2_activity_breadth_by_group_sql(silver_sales, task2_product_performance),
    comment=(
        "Task 2: activity-breadth tiers (months with sales: no sales / 1 / 2 to 5 / "
        "6 to N-1 / all N, N re-derived from the data window on every refresh) per "
        "performance group, with product counts and within-group shares. Quantifies "
        "how closely the decile cut follows activity breadth. Materialized view, "
        "refreshed on every gold run."
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row counts

# COMMAND ----------

gold_entities = (
    "task1_sales_facts",
    "task1_sales_rollup",
    "task1_discount_terms",
    "task1_filter_options",
    "task2_product_performance",
    "task2_performance_summary",
    "task2_top_10_vs_bottom_10_attribute_deltas",
    "task2_monthly_patterns",
    "task2_sales_timing_summary",
    "task2_month_share_deltas",
    "task2_activity_breadth_by_group",
)

for entity in gold_entities:
    full_name = table_name("gold", entity)
    print(f"{full_name}: {spark.table(full_name).count()} rows")
