# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,03 — Gold Aggregations
# MAGIC %md
# MAGIC # 03 — Gold Aggregations
# MAGIC Publishes the Task 1 sale-grain fact view (`sales_data_cc.gold.task1_sales_facts`, the single
# MAGIC gold home of the sales-products-discounts join) and its pre-aggregated
# MAGIC (section, season, sale_month, city) rollup, and computes Task 2 (top 10% vs bottom 10%
# MAGIC product groups, attribute deltas, monthly patterns, and sales-timing analyses) from
# MAGIC silver. Gold is always rebuilt from silver, so it has no load_mode of its own.

# COMMAND ----------

# MAGIC %run ./common/config

# COMMAND ----------

# MAGIC %run ./common/gold

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read silver inputs

# COMMAND ----------

silver_products = spark.table(table_name("silver", "products"))
silver_sales = spark.table(table_name("silver", "sales"))
silver_discounts = spark.table(table_name("silver", "discounts"))

# Full-data sizing context: ~20K products / ~184K silver discounts / ~1M sales.
# No explicit broadcast: every reference join side is well under Spark's default
# ~10 MB auto-broadcast threshold, so Spark broadcasts them automatically. If silver
# grows substantially, re-verify sizes before assuming automatic broadcast applies.
# These silver tables feed the Task 1 fact view (the join lives in gold once) and
# the Task 2 aggregations.

# COMMAND ----------

# DBTITLE 1,Task 1 — sale-grain fact view and rollup
# MAGIC %md
# MAGIC ## Task 1 — sale-grain fact view and rollup
# MAGIC `sales_data_cc.gold.task1_sales_facts` is a view with one row per sale: city, product_id,
# MAGIC pieces_sold, price, discount, section, season, sale_month, sale_ts, and the
# MAGIC effective_sales_volume business metric (`pieces_sold * price * (1 - discount)`). The
# MAGIC sales-products-discounts join lives in gold once; the view is the semantic source of
# MAGIC truth, the build input for the rollup below, and the dashboard's source for the one
# MAGIC metric that cannot be re-combined from the rollup — the exact per-city count of
# MAGIC distinct products sold. A view is free: no rebuild schedule, always reads silver live.
# MAGIC
# MAGIC `sales_data_cc.gold.task1_sales_rollup` pre-aggregates the same facts at
# MAGIC (section, season, sale_month, city) grain so heavy slices read a small table instead
# MAGIC of ~1M sale rows: additive pieces/volume metrics plus `gross_sales_volume` and
# MAGIC `max_discount` per grain. Any selection's city-max discount is `MAX(max_discount)`
# MAGIC over the selected rollup rows, and its max-discount volume is
# MAGIC `SUM(gross_sales_volume) * (1 - city_max_discount)` — both re-combine exactly, so
# MAGIC the Task 1 dashboard's city dataset reads this table for its volume, pieces and
# MAGIC city-max metrics. `distinct_product_count` is exact
# MAGIC only at this grain — for exact per-city distinct counts, use the fact view.

# COMMAND ----------

# DBTITLE 1,Task 1 — sale-grain fact view
task1_fact_view = table_name("gold", "task1_sales_facts")

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {task1_fact_view} AS
    SELECT
        s.city,
        s.product_id,
        s.pieces_sold,
        p.price,
        COALESCE(d.discount, 0) AS discount,
        p.section,
        p.season,
        s.sale_month,
        s.sale_ts,
        s.pieces_sold * p.price * (1 - COALESCE(d.discount, 0)) AS effective_sales_volume
    FROM {table_name("silver", "sales")} s
        JOIN {table_name("silver", "products")} p ON s.product_id = p.product_id
        LEFT JOIN {table_name("silver", "discounts")} d
            ON s.product_id = d.product_id AND s.city = d.city
    """
)
spark.sql(
    "COMMENT ON VIEW " + task1_fact_view + " IS "
    "'Task 1: one row per sale — city, product_id, pieces_sold, price, actual discount "
    "(0 when none applies), section, season, sale_month, sale_ts, and "
    "effective_sales_volume = pieces_sold * price * (1 - discount). The single gold "
    "home of the sales-products-discounts join; dashboards filter this view directly, "
    "so per-selection metrics (e.g. each city’s max discount within the current "
    "filter) are computed over exactly the selected rows. Always fresh: reads silver "
    "live, no rebuild schedule.'"
)
print(f"Created view {task1_fact_view}")

# COMMAND ----------

# DBTITLE 1,Task 1 — rollup by section, season, sale month, city
# Pre-aggregated rollup of the fact view at (section, season, sale_month, city)
# grain. Written as a liquid-clustered gold table in the persist cell below.
# Why pre-aggregate here (performance): every dashboard query at this grain would
# otherwise re-run the sales-products-discounts join and aggregate ~1M sale-grain
# rows on the fly. Aggregating once at write time collapses that to a tiny table
# (one row per grain cell), so heavy dashboard slices scan/prune a few thousand rows
# instead of scanning the full fact view, and the additive metrics (sums, max,
# counts) can be re-combined over any selection without touching raw sales again.
# Only exact distinct counts don't survive re-aggregation (see the notes on
# distinct_product_count below and in the fact view cell).
task1_rollup = (
    spark.table(task1_fact_view)
    .groupBy("section", "season", "sale_month", "city")
    .agg(
        F.count(F.lit(1)).alias("sale_rows"),
        F.sum("pieces_sold").alias("total_pieces_sold"),
        F.sum(F.col("pieces_sold") * F.col("price")).alias("gross_sales_volume"),
        F.sum("effective_sales_volume").alias("total_effective_sales_volume"),
        F.max("discount").alias("max_discount"),
        F.countDistinct("product_id").alias("distinct_product_count"),
    )
)
display(task1_rollup.orderBy("section", "season", "sale_month", "city"))

# COMMAND ----------

# DBTITLE 1,Task 2 — top 10% vs bottom 10% product groups
# MAGIC %md
# MAGIC ## Task 2 — top 10% vs bottom 10% product groups
# MAGIC Products are ranked by raw lifetime pieces sold across all cities and dates, then split
# MAGIC into percentile groups with `percent_rank()` (0 = fewest pieces, 1 = most pieces):
# MAGIC
# MAGIC - **top 10%** — `percentile_rank >= 0.90`
# MAGIC - **middle 80%** — the comparison base between the two cut-offs
# MAGIC - **bottom 10%** — `percentile_rank <= 0.10` (includes any unsold catalog products)
# MAGIC
# MAGIC Why percentile groups instead of the previous best/worst extremes: the extremes were
# MAGIC exact ties (one large tie at the maximum, one at the minimum) — degenerate ~1% slices
# MAGIC of the catalog. Deciles are statistically meaningful cohorts (~2,000 products each), and
# MAGIC tied products share a percentile value, so a tie spanning a cut-off pulls the whole tie
# MAGIC into one group: deterministic, unlike `NTILE(10)`, which would split boundary ties
# MAGIC arbitrarily. Compare the groups as **mixes** (how the top-decile population differs
# MAGIC from the bottom-decile population), not as identities of individual products. No
# MAGIC city/discount analysis is included in Task 2 at this stage.
# MAGIC
# MAGIC The seven tables below answer, in order:
# MAGIC 1. `task2_performance_summary` — how big are the groups and does price differ between them?
# MAGIC 2. `task2_product_performance` — per-product pieces sold, with its percentile rank and group.
# MAGIC 3. `task2_top_10_vs_bottom_10_attribute_deltas` — which attribute values (position,
# MAGIC    season, material, terms, promotion, origin) separate the top 10% from the bottom 10%,
# MAGIC    measured as within-group share deltas in percentage points?
# MAGIC 4. `task2_monthly_patterns` — monthly pieces by calendar sale month and catalog season
# MAGIC    per group.
# MAGIC 5. `task2_sales_timing_summary` — how consistently does each group sell across months
# MAGIC    (active months, share selling in all eleven vs a single month, pieces per active
# MAGIC    month, in-season share)?
# MAGIC 6. `task2_month_share_deltas` — do any calendar months separate the top 10% from the
# MAGIC    bottom 10% (within-group monthly share delta)?
# MAGIC 7. `task2_activity_breadth_by_group` — how closely does the decile cut follow activity
# MAGIC    breadth (months with sales)?

# COMMAND ----------

product_performance = rank_product_performance(
    silver_sales, silver_products, group_fraction=0.10
)
performance_summary = product_performance_summary(product_performance)
attribute_deltas = product_top_10_vs_bottom_10_attribute_deltas(product_performance)
monthly_patterns = monthly_sales_patterns(silver_sales, product_performance)
timing_summary = product_sales_timing_summary(
    silver_sales, product_performance, monthly_patterns
)
month_share_deltas = product_month_share_deltas(monthly_patterns)
activity_breadth = activity_breadth_by_performance_group(
    silver_sales, product_performance
)



# COMMAND ----------

# MAGIC %md
# MAGIC **Catalog list price caveat:** the price comparison uses the listed product price.
# MAGIC Actual discounts can vary by product and city; they are not incorporated in this Task 2
# MAGIC analysis, so these metrics are not the price customers paid or realized revenue.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Table — task2_performance_summary
# MAGIC %md
# MAGIC #### Table: `sales_data_cc.gold.task2_performance_summary`
# MAGIC
# MAGIC Task 2: product counts, pieces-sold statistics, and catalog list-price statistics by
# MAGIC performance group (top 10% / middle 80% / bottom 10% by lifetime pieces sold). List
# MAGIC prices exclude discounts, so they are not realized revenue. Rebuilt from silver on
# MAGIC every gold run.
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

# DBTITLE 1,Task 2 — performance summary
display(performance_summary)

# COMMAND ----------

# DBTITLE 1,Table — task2_product_performance (top/bottom samples)
# MAGIC %md
# MAGIC #### Table: `sales_data_cc.gold.task2_product_performance` (top/bottom samples)
# MAGIC
# MAGIC Task 2: per-product total pieces sold across all cities and dates, with a
# MAGIC `percentile_rank` (0 = fewest pieces, 1 = most pieces) and a `performance_group`
# MAGIC label: `top 10%`, `middle 80%`, or `bottom 10%`. Unsold products are included with
# MAGIC zero pieces and fall in the bottom group. Rebuilt from silver on every gold run.
# MAGIC Shown below: the 10 highest- and 10 lowest-selling products, as samples of the two
# MAGIC comparison groups.
# MAGIC
# MAGIC **What to observe:**
# MAGIC - These rows are samples of the decile mixes, not individual winners: scan `section`,
# MAGIC   `season`, `terms`, `promotion`, and `origin` to see what each group looks like
# MAGIC   before reading the aggregated comparison in
# MAGIC   `task2_top_10_vs_bottom_10_attribute_deltas`.

# COMMAND ----------

# DBTITLE 1,Task 2 — top/bottom group samples
# Samples of the two comparison groups: the 10 highest- and 10 lowest-selling
# products (product_id breaks ties deterministically).
top_sellers_sample = (
    product_performance.orderBy(F.desc("total_pieces_sold"), "product_id").limit(10)
)
bottom_sellers_sample = (
    product_performance.orderBy("total_pieces_sold", "product_id").limit(10)
)
display(top_sellers_sample.unionByName(bottom_sellers_sample))

# COMMAND ----------

# DBTITLE 1,Table — task2_top_10_vs_bottom_10_attribute_deltas
# MAGIC %md
# MAGIC #### Table: `sales_data_cc.gold.task2_top_10_vs_bottom_10_attribute_deltas`
# MAGIC
# MAGIC Task 2: for every varying categorical product attribute value, the product counts and
# MAGIC within-group shares for the top 10% and bottom 10% seller groups, plus the share
# MAGIC delta. `top_10_percent_share_of_group` and `bottom_10_percent_share_of_group` are
# MAGIC fractions of the respective group (0-1); `share_delta_percentage_points` = (top share
# MAGIC minus bottom share) x 100, so +3.2 means the value is 3.2 percentage points more
# MAGIC common among top sellers than bottom sellers; `skews_towards` names the group with the
# MAGIC larger share. Rebuilt from silver on every gold run.
# MAGIC
# MAGIC All attribute values are stored, including weak separators — "no signal" (e.g.
# MAGIC promotion) is a finding, not noise. Presentation layers select strong patterns with
# MAGIC `ABS(share_delta_percentage_points) >= 1` ("patterns above 1 percentage point").
# MAGIC
# MAGIC **What to observe (rows are ordered by the magnitude of `share_delta_percentage_points`):**
# MAGIC - The largest absolute deltas are the attribute values that separate the deciles;
# MAGIC   values near zero (e.g. promotion in previous rebuilds) do not separate them.
# MAGIC - Constant attributes (brand, product category, currency) are excluded from this
# MAGIC   table: they cannot discriminate, so every group would show ~100% for them.
# MAGIC - A value that never appears in one group is stored with a genuine 0% share of that
# MAGIC   group (`top_10_percent_product_count` / `bottom_10_percent_product_count` = 0).

# COMMAND ----------

# DBTITLE 1,Task 2 — attribute deltas
display(attribute_deltas)

# COMMAND ----------

# DBTITLE 1,Table — task2_monthly_patterns
# MAGIC %md
# MAGIC #### Table: `sales_data_cc.gold.task2_monthly_patterns`
# MAGIC
# MAGIC Task 2: monthly pieces sold and within-group share by calendar sale month, product
# MAGIC catalog season, and performance group (top 10% / middle 80% / bottom 10% by lifetime
# MAGIC pieces sold). Rebuilt from silver on every gold run.
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

# DBTITLE 1,Task 2 — monthly patterns
display(monthly_patterns)

# COMMAND ----------

# DBTITLE 1,Sales-timing analyses
# MAGIC %md
# MAGIC ### Sales-timing analyses: consistency, monthly deltas, activity breadth
# MAGIC
# MAGIC Three tables extend the sales-month analysis beyond absolute volumes, which the
# MAGIC middle 80%'s scale would otherwise drown out:
# MAGIC
# MAGIC 1. `task2_sales_timing_summary` — per group: how many calendar months its products
# MAGIC    sell in (`average_active_sale_months_per_product`), the share of products selling
# MAGIC    in a single month vs in all of the data window's sale months (the month count is
# MAGIC    derived from the data at each rebuild — currently 11, no December), average
# MAGIC    pieces per active month (intensity vs breadth), and the share of the group's
# MAGIC    pieces sold in calendar months matching the product's catalog season.
# MAGIC 2. `task2_month_share_deltas` — per calendar month, the top 10%'s share of its own
# MAGIC    pieces vs the bottom 10%'s share of its own, with the delta in percentage points.
# MAGIC    A near-zero delta is the finding that no month separates the deciles.
# MAGIC 3. `task2_activity_breadth_by_group` — months-with-sales tiers per group. The decile
# MAGIC    cut largely follows activity breadth: products with few active months concentrate
# MAGIC    in the bottom 10%, so the attribute deltas above read as combined
# MAGIC    appeal-and-consistency findings rather than pure attribute effects.

# COMMAND ----------

# DBTITLE 1,Task 2 — sales-timing summary
display(timing_summary)

# COMMAND ----------

# DBTITLE 1,Task 2 — monthly share deltas
display(month_share_deltas)

# COMMAND ----------

# DBTITLE 1,Task 2 — activity breadth by group
display(activity_breadth)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist gold tables

# COMMAND ----------

# DBTITLE 1,Persist gold tables
# Rollup of the fact view at (section, season, sale_month, city) grain, liquid
# clustered on the dashboard's filter keys so selection scans prune.
rollup_table = table_name("gold", "task1_sales_rollup")
task1_rollup.createOrReplaceTempView("task1_rollup_to_write")
spark.sql(
    f"CREATE OR REPLACE TABLE {rollup_table} "
    "CLUSTER BY (section, season, sale_month) "
    "AS SELECT * FROM task1_rollup_to_write"
)
spark.sql(
    "COMMENT ON TABLE " + rollup_table + " IS "
    "'Task 1: rollup of task1_sales_facts at (section, season, sale_month, city) grain "
    "with additive pieces/volume metrics, gross_sales_volume, and the max discount "
    "within each grain cell. Any selection’s city-max discount is MAX(max_discount) "
    "over the selected rollup rows and its max-discount volume is "
    "SUM(gross_sales_volume) * (1 - city_max_discount); distinct_product_count is "
    "exact only at this grain (use the fact view for per-city distinct counts). "
    "Rebuilt from silver on every gold run.'"
)

gold_entities = ["task1_sales_facts", "task1_sales_rollup"]

write_gold_table(
    product_performance,
    table_name("gold", "task2_product_performance"),
    # TB introduced only if table grows too large
    # partition_columns=("section", "season"), 
    comment=(
        "Task 2: per-product total pieces sold across all cities and dates, with a "
        "percentile_rank (0 = fewest pieces, 1 = most pieces) and a top 10% / middle "
        "80% / bottom 10% performance group from percent_rank over lifetime pieces. "
        "Ties share a percentile and stay in one group; unsold products are included "
        "with zero pieces and fall in the bottom group. Rebuilt from silver on every "
        "gold run."
    ),
)
write_gold_table(
    performance_summary,
    table_name("gold", "task2_performance_summary"),
    comment=(
        "Task 2: product counts, pieces-sold statistics, and catalog list-price "
        "statistics by performance group. List prices exclude discounts, so they are "
        "not realized revenue. Rebuilt from silver on every gold run."
    ),
)
# task2_attribute_patterns (the old best/worst within-group percentages) is
# superseded by task2_top_10_vs_bottom_10_attribute_deltas; drop the stale table
# so gold keeps exactly one Task 2 attribute table.
spark.sql(
    "DROP TABLE IF EXISTS " + table_name("gold", "task2_attribute_patterns")
)
write_gold_table(
    attribute_deltas,
    table_name("gold", "task2_top_10_vs_bottom_10_attribute_deltas"),
    comment=(
        "Task 2: within-group attribute shares for the top 10% vs bottom 10% seller "
        "groups, with the share delta in percentage points (top share minus bottom "
        "share; positive = over-represented among top sellers). All attribute values "
        "are kept, weak separators included; filter ABS(share_delta_percentage_points) "
        ">= 1 for strong patterns. Rebuilt from silver on every gold run."
    ),
)
write_gold_table(
    monthly_patterns,
    table_name("gold", "task2_monthly_patterns"),
    comment=(
        "Task 2: monthly pieces sold and within-group share by calendar sale month, "
        "product catalog season, and performance group (top 10% / middle 80% / "
        "bottom 10% by lifetime pieces sold). Rebuilt from silver on every gold run."
    ),
)
write_gold_table(
    timing_summary,
    table_name("gold", "task2_sales_timing_summary"),
    comment=(
        "Task 2: per-group sales-timing metrics — average active sale months per "
        "product, share of products selling in a single month vs in all of the "
        "data window's sale months (the count is derived at each rebuild), "
        "average pieces sold per active month, and the "
        "share of the group's pieces sold in calendar months matching the product's "
        "catalog season. Rebuilt from silver on every gold run."
    ),
)
write_gold_table(
    month_share_deltas,
    table_name("gold", "task2_month_share_deltas"),
    comment=(
        "Task 2: per calendar sale month, the top 10% and bottom 10% shares of "
        "their own lifetime pieces, the share delta in percentage points (top minus "
        "bottom), and which group the month skews towards. Rebuilt from silver on "
        "every gold run."
    ),
)
write_gold_table(
    activity_breadth,
    table_name("gold", "task2_activity_breadth_by_group"),
    comment=(
        "Task 2: activity-breadth tiers (months with sales: no sales / 1 / 2 to 5 / "
        "6 to N-1 / all N, N derived from the data window) per performance group, "
        "with product counts and "
        "within-group shares. Quantifies how closely the decile cut follows "
        "activity breadth. Rebuilt from silver on every gold run."
    ),
)
gold_entities.extend(
    (
        "task2_product_performance",
        "task2_performance_summary",
        "task2_top_10_vs_bottom_10_attribute_deltas",
        "task2_monthly_patterns",
        "task2_sales_timing_summary",
        "task2_month_share_deltas",
        "task2_activity_breadth_by_group",
    )
)

for entity in gold_entities:
    full_name = table_name("gold", entity)
    print(f"{full_name}: {spark.table(full_name).count()} rows")