# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ### common/gold
# MAGIC SQL builders for every gold object (Task 1 and Task 2), pulled in by
# MAGIC `03_gold_aggregations.py` via `%run`. Do not run this notebook standalone.
# MAGIC
# MAGIC Every gold object is a Databricks **materialized view**: each function below
# MAGIC returns the `SELECT` text for one MV, parameterized by its upstream objects'
# MAGIC fully-qualified names so the chain reads gold-on-gold wherever an upstream gold
# MAGIC object already exists (no object re-joins silver that an earlier gold object
# MAGIC already joined). `ensure_materialized_view` creates an MV the first time it
# MAGIC doesn't exist yet and otherwise only refreshes it — see that function's
# MAGIC docstring for why, and for the one thing it does **not** handle automatically
# MAGIC (picking up a changed SQL definition on an MV that already exists).

# COMMAND ----------

# DBTITLE 1,Shared labels and attribute list
from typing import Optional, Tuple

# Single source of truth for the percentile cut and its labels: every MV's SQL and
# every downstream MV that filters on performance_group interpolates these instead
# of repeating the literals. (Dashboard JSON cannot import these constants — see
# DESIGN-NOTES.md for the resulting literal-string coupling in the Lakeview files.)
GROUP_FRACTION = 0.10
BOTTOM_GROUP_LABEL = f"bottom {round(GROUP_FRACTION * 100)}%"
TOP_GROUP_LABEL = f"top {round(GROUP_FRACTION * 100)}%"
MIDDLE_GROUP_LABEL = f"middle {round((1 - 2 * GROUP_FRACTION) * 100)}%"

# Only attributes that vary across the catalog can separate the top group from the
# bottom group; brand (always Zara), product_category (always clothing), and currency
# are constant and would show ~100% in every group, so they are excluded.
PRODUCT_CATEGORICAL_ATTRIBUTES = (
    "seasonal",
    "season",
    "section",
    "terms",
    "material",
    "product_position",
    "promotion",
    "origin",
)

# COMMAND ----------

# DBTITLE 1,Materialized view DDL helper
def ensure_materialized_view(
    full_view_name: str,
    select_sql: str,
    cluster_by: Optional[Tuple[str, ...]] = None,
    comment: Optional[str] = None,
) -> None:
    """Create a materialized view the first time it doesn't exist; otherwise just refresh it.

    The DDL (`CREATE MATERIALIZED VIEW ... AS SELECT ...`) only runs once, the first
    time this MV is built — every later gold run finds the object already exists and
    issues a plain `REFRESH MATERIALIZED VIEW` instead. This matters because
    redefining an MV's DDL on every run (`CREATE OR REPLACE`) can make Databricks
    treat it as a new object and discard whatever incremental-refresh state it was
    tracking, which would silently turn every refresh back into a full recompute —
    the opposite of what an MV is for. Refresh-only runs let that incremental state
    persist across runs for the MVs that can actually use it (the additive
    aggregates); the window-function-driven Task 2 chain still fully recomputes on
    every refresh regardless, since `PERCENT_RANK()` over the whole catalog has no
    incremental form to preserve.

    Caveat this trades away: because the CREATE only fires once, changing an MV's
    `select_sql`/`cluster_by`/`comment` in this file does **not** retroactively
    change an MV that already exists — a plain pipeline run keeps refreshing the
    old definition forever. Apply a changed definition by dropping the MV once
    (`DROP MATERIALIZED VIEW <name>`) before the next run, so this function
    recreates it from the current code.
    """
    if spark.catalog.tableExists(full_view_name):
        spark.sql(f"REFRESH MATERIALIZED VIEW {full_view_name}")
        print(f"Refreshed materialized view {full_view_name}")
        return

    clauses = [f"CREATE MATERIALIZED VIEW {full_view_name}"]
    if cluster_by:
        clauses.append(f"CLUSTER BY ({', '.join(cluster_by)})")
    if comment:
        escaped_comment = comment.replace("'", "''")
        clauses.append(f"COMMENT '{escaped_comment}'")
    clauses.append(f"AS\n{select_sql}")
    spark.sql("\n".join(clauses))
    print(f"Created materialized view {full_view_name}")

# COMMAND ----------

# DBTITLE 1,Task 1 — sale-grain facts and rollup
def task1_sales_facts_sql(
    silver_sales: str, silver_products: str, silver_discounts: str
) -> str:
    """One row per sale: city, product_id, pieces_sold, price, discount, section,
    season, sale_month, sale_ts, and effective_sales_volume. The single gold home
    of the sales-products-discounts join."""
    return f"""
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
    FROM {silver_sales} s
        JOIN {silver_products} p ON s.product_id = p.product_id
        LEFT JOIN {silver_discounts} d
            ON s.product_id = d.product_id AND s.city = d.city
    """


def task1_sales_rollup_sql(task1_sales_facts: str) -> str:
    """Pre-aggregate of task1_sales_facts at (section, season, sale_month, city)
    grain: additive pieces/volume metrics, gross_sales_volume, and per-cell max
    discount."""
    return f"""
    SELECT
        section,
        season,
        sale_month,
        city,
        COUNT(*) AS sale_rows,
        SUM(pieces_sold) AS total_pieces_sold,
        SUM(pieces_sold * price) AS gross_sales_volume,
        SUM(effective_sales_volume) AS total_effective_sales_volume,
        MAX(discount) AS max_discount,
        COUNT(DISTINCT product_id) AS distinct_product_count
    FROM {task1_sales_facts}
    GROUP BY section, season, sale_month, city
    """


def task1_discount_terms_sql(
    silver_discounts: str, silver_products: str, silver_sales: str
) -> str:
    """One row per (product_id, city, sale_month-or-NULL) discount term: the
    product's section/season, its raw discount and 1%-granularity discount_pct, and
    the calendar sale month it was sold in (NULL when that product/city pair never
    sold). The single source for both of the dashboard's discount-distribution
    widgets; they bucket this 1%-granularity grain into 5% buckets themselves at
    query time rather than reading a second, near-duplicate aggregate."""
    return f"""
    SELECT
        d.product_id,
        d.city,
        p.section,
        p.season,
        d.discount,
        CAST(ROUND(d.discount * 100) AS INT) AS discount_pct,
        k.sale_month
    FROM {silver_discounts} d
        JOIN {silver_products} p ON d.product_id = p.product_id
        LEFT JOIN (
            SELECT DISTINCT product_id, city, sale_month FROM {silver_sales}
        ) k
            ON k.product_id = d.product_id AND k.city = d.city
    """


def task1_filter_options_sql(silver_products: str, silver_sales: str) -> str:
    """Long/narrow lookup of (filter_type, filter_value): the distinct section,
    season, and sale_month values used to populate the Task 1 dashboard's filter
    widgets, as one gold object shared by all three filter dropdowns."""
    return f"""
    SELECT 'section' AS filter_type, section AS filter_value
    FROM {silver_products}
    GROUP BY section
    UNION ALL
    SELECT 'season' AS filter_type, season AS filter_value
    FROM {silver_products}
    GROUP BY season
    UNION ALL
    SELECT 'sale_month' AS filter_type, sale_month AS filter_value
    FROM {silver_sales}
    GROUP BY sale_month
    """

# COMMAND ----------

# DBTITLE 1,Task 2 — product performance ranking
def task2_product_performance_sql(
    silver_sales: str,
    silver_products: str,
    group_fraction: float = GROUP_FRACTION,
    bottom_label: str = BOTTOM_GROUP_LABEL,
    top_label: str = TOP_GROUP_LABEL,
    middle_label: str = MIDDLE_GROUP_LABEL,
) -> str:
    """Classify every catalog product into top/middle/bottom percentile groups by
    lifetime pieces sold.

    `PERCENT_RANK()` orders products by total pieces sold (0 = fewest pieces,
    1 = most pieces); the bottom group is rank <= group_fraction, the top group is
    rank >= 1 - group_fraction, everything else is the middle comparison base. Tied
    products share a percentile value, so a tie spanning a cut-off pulls the whole
    tie into one group — deterministic, unlike NTILE, which would split boundary
    ties arbitrarily. The LEFT JOIN from the full product catalog zero-fills unsold
    products instead of dropping them from the ranking, so they rank into the
    bottom group.
    """
    return f"""
    WITH totals AS (
    
        SELECT
            product_id,
            SUM(pieces_sold) AS total_pieces_sold,
            COUNT(DISTINCT city) AS selling_city_count,
            MIN(sale_ts) AS first_sale_ts,
            MAX(sale_ts) AS last_sale_ts
        FROM {silver_sales}
        GROUP BY product_id
    ),
    performance AS (
        SELECT
            p.*,
            COALESCE(t.total_pieces_sold, 0) AS total_pieces_sold,
            COALESCE(t.selling_city_count, 0) AS selling_city_count,
            t.first_sale_ts,
            t.last_sale_ts
        FROM {silver_products} p
            LEFT JOIN totals t ON p.product_id = t.product_id
    ),
    ranked AS (
        SELECT
            *,
            PERCENT_RANK() OVER (ORDER BY total_pieces_sold) AS percentile_rank
        FROM performance
    )
    SELECT
        *,
        CASE
            WHEN percentile_rank <= {group_fraction} THEN '{bottom_label}'
            WHEN percentile_rank >= {1 - group_fraction} THEN '{top_label}'
            ELSE '{middle_label}'
        END AS performance_group
    FROM ranked
    """

# COMMAND ----------

# DBTITLE 1,Task 2 summaries
# MAGIC %md
# MAGIC ## Task 2 summaries
# MAGIC Products are grouped into top 10% / middle 80% / bottom 10% by lifetime pieces sold
# MAGIC (see `task2_product_performance_sql`). Mix comparisons use within-group percentages so
# MAGIC different group sizes never distort them; the attribute table stores the top-vs-bottom
# MAGIC share delta in percentage points. Monthly sales retain the product's catalog season,
# MAGIC separate from the calendar month when the sale occurred. Timing analyses extend the
# MAGIC monthly view with per-product sales consistency (months with sales), within-group
# MAGIC monthly share deltas, and an activity-breadth cross-tab against the seller groups.

# COMMAND ----------

def task2_performance_summary_sql(task2_product_performance: str) -> str:
    """Product counts, pieces sold, and catalog list price by seller group."""
    return f"""
    SELECT
        performance_group,
        COUNT(DISTINCT product_id) AS product_count,
        SUM(total_pieces_sold) AS total_pieces_sold,
        AVG(total_pieces_sold) AS average_pieces_per_product,
        MIN(total_pieces_sold) AS minimum_pieces_per_product,
        MAX(total_pieces_sold) AS maximum_pieces_per_product,
        AVG(price) AS average_catalog_list_price,
        PERCENTILE_APPROX(price, 0.5) AS median_catalog_list_price
    FROM {task2_product_performance}
    GROUP BY performance_group
    ORDER BY performance_group
    """


def task2_attribute_deltas_sql(
    task2_product_performance: str,
    top_group: str = TOP_GROUP_LABEL,
    bottom_group: str = BOTTOM_GROUP_LABEL,
) -> str:
    """Compare within-group attribute shares between the top 10% and bottom 10%
    sellers.

    Every varying catalog attribute value gets one row: the number of top-group and
    bottom-group products carrying that value, each group's share of its own
    products (0-1), the share delta in percentage points (top share minus bottom
    share), and which group the value skews towards. Shares are computed within
    each group, so the two group sizes never distort the comparison; a value
    absent from one group is a genuine 0% share of that group.

    All attribute values are kept, including barely-separating ones — "no signal"
    (e.g. promotion) is a finding, not noise. Presentation layers select strong
    patterns by filtering ABS(share_delta_percentage_points) (e.g. >= 1 percentage
    point). The middle 80% is the comparison base in task2_performance_summary only.
    """
    attribute_union = "\n        UNION ALL\n        ".join(
        f"""SELECT
            product_id,
            performance_group,
            '{attribute}' AS attribute_name,
            CASE
                WHEN TRIM(CAST({attribute} AS STRING)) IS NULL
                    OR TRIM(CAST({attribute} AS STRING)) = '' THEN 'Unknown'
                ELSE TRIM(CAST({attribute} AS STRING))
            END AS attribute_value
        FROM comparison"""
        for attribute in PRODUCT_CATEGORICAL_ATTRIBUTES
    )
    return f"""
    WITH comparison AS (
        SELECT *
        FROM {task2_product_performance}
        WHERE performance_group IN ('{top_group}', '{bottom_group}')
    ),
    group_sizes AS (
        SELECT performance_group, COUNT(DISTINCT product_id) AS group_product_count
        FROM comparison
        GROUP BY performance_group
    ),
    attribute_rows AS (
        {attribute_union}
    ),
    counts AS (
        SELECT
            performance_group,
            attribute_name,
            attribute_value,
            COUNT(DISTINCT product_id) AS product_count
        FROM attribute_rows
        GROUP BY performance_group, attribute_name, attribute_value
    ),
    shares AS (
        SELECT
            c.attribute_name,
            c.attribute_value,
            c.performance_group,
            c.product_count,
            c.product_count / g.group_product_count AS group_share
        FROM counts c
            JOIN group_sizes g ON c.performance_group = g.performance_group
    ),
    grouped AS (
        SELECT
            attribute_name,
            attribute_value,
            COALESCE(
                MAX(CASE WHEN performance_group = '{top_group}' THEN product_count END), 0
            ) AS top_10_percent_product_count,
            COALESCE(
                MAX(CASE WHEN performance_group = '{top_group}' THEN group_share END), 0.0
            ) AS top_10_percent_share_of_group,
            COALESCE(
                MAX(CASE WHEN performance_group = '{bottom_group}' THEN product_count END), 0
            ) AS bottom_10_percent_product_count,
            COALESCE(
                MAX(CASE WHEN performance_group = '{bottom_group}' THEN group_share END), 0.0
            ) AS bottom_10_percent_share_of_group
        FROM shares
        GROUP BY attribute_name, attribute_value
    )
    SELECT
        attribute_name,
        attribute_value,
        top_10_percent_product_count,
        top_10_percent_share_of_group,
        bottom_10_percent_product_count,
        bottom_10_percent_share_of_group,
        (top_10_percent_share_of_group - bottom_10_percent_share_of_group) * 100.0
            AS share_delta_percentage_points,
        CASE
            WHEN top_10_percent_share_of_group >= bottom_10_percent_share_of_group
                THEN '{top_group}'
            ELSE '{bottom_group}'
        END AS skews_towards
    FROM grouped
    ORDER BY
        ABS((top_10_percent_share_of_group - bottom_10_percent_share_of_group) * 100.0) DESC,
        attribute_name,
        attribute_value
    """


def task2_monthly_patterns_sql(
    silver_sales: str, task2_product_performance: str
) -> str:
    """Monthly pieces and share by calendar month, product season, and seller
    group."""
    return f"""
    WITH classified AS (
        SELECT
            s.product_id,
            s.sale_month,
            s.pieces_sold,
            pp.performance_group,
            pp.season
        FROM {silver_sales} s
            LEFT JOIN {task2_product_performance} pp ON s.product_id = pp.product_id
    ),
    monthly AS (
        SELECT
            sale_month,
            season,
            performance_group,
            SUM(pieces_sold) AS total_pieces_sold,
            COUNT(DISTINCT product_id) AS product_count
        FROM classified
        GROUP BY sale_month, season, performance_group
    )
    SELECT
        sale_month,
        season,
        performance_group,
        total_pieces_sold,
        product_count,
        SUM(total_pieces_sold) OVER (PARTITION BY season, performance_group)
            AS group_season_total_pieces,
        total_pieces_sold * 100.0
            / SUM(total_pieces_sold) OVER (PARTITION BY season, performance_group)
            AS group_season_pieces_pct
    FROM monthly
    ORDER BY sale_month, season, performance_group
    """


def task2_sales_timing_summary_sql(
    silver_sales: str,
    task2_product_performance: str,
    task2_monthly_patterns: str,
) -> str:
    """Per-group sales-timing metrics: consistency, intensity, and in-season share.

    Answers the sales-month half of Task 2 with three complementary metrics.
    (1) Consistency: how many calendar months each product sells in — the deciles'
    strongest timing separator, because total pieces scale with months on sale.
    (2) Intensity: average pieces sold per active month, which separates scale from
    breadth. (3) In-season alignment: the share of a group's pieces sold in
    calendar months matching the product's catalog season, computed from
    task2_monthly_patterns (sale month and catalog season are independent
    dimensions; the data window has no December, so winter-catalog products only
    meet January and February).

    The data window's number of distinct sale months (`total_sale_months`) is a
    scalar subquery re-evaluated on every refresh, so
    share_of_products_selling_in_all_sale_months stays correct if the window gains
    or loses months — it is never hard-coded into this view's text. Products with
    no sales (zero-filled in task2_product_performance) enter with 0 active months
    and contribute 0 to the single-month and all-months shares; the
    pieces-per-month average only covers products with at least one active month.
    """
    return f"""
    WITH per_product_months AS (
        SELECT
            product_id,
            COUNT(DISTINCT sale_month) AS active_sale_months,
            SUM(pieces_sold) AS product_total_pieces_sold
        FROM {silver_sales}
        GROUP BY product_id
    ),
    breadth AS (
        SELECT
            pp.product_id,
            pp.performance_group,
            COALESCE(ppm.active_sale_months, 0) AS active_sale_months,
            COALESCE(ppm.product_total_pieces_sold, 0) AS product_total_pieces_sold
        FROM {task2_product_performance} pp
            LEFT JOIN per_product_months ppm ON pp.product_id = ppm.product_id
    ),
    total_months AS (
        SELECT COUNT(DISTINCT sale_month) AS total_sale_months FROM {silver_sales}
    ),
    timing AS (
        SELECT
            b.performance_group,
            COUNT(*) AS product_count,
            ROUND(AVG(b.active_sale_months), 2) AS average_active_sale_months_per_product,
            ROUND(
                AVG(CASE WHEN b.active_sale_months = 1 THEN 1.0 ELSE 0.0 END), 4
            ) AS share_of_products_selling_in_a_single_month,
            ROUND(
                AVG(
                    CASE
                        WHEN b.active_sale_months = tm.total_sale_months THEN 1.0
                        ELSE 0.0
                    END
                ), 4
            ) AS share_of_products_selling_in_all_sale_months,
            ROUND(
                AVG(
                    CASE
                        WHEN b.active_sale_months > 0
                            THEN b.product_total_pieces_sold / b.active_sale_months
                    END
                ), 2
            ) AS average_pieces_sold_per_active_month
        FROM breadth b
            CROSS JOIN total_months tm
        GROUP BY b.performance_group
    ),
    monthly_with_season AS (
        SELECT
            performance_group,
            season,
            total_pieces_sold,
            CASE
                WHEN MONTH(TO_DATE(CONCAT(sale_month, '-01'))) IN (3, 4, 5) THEN 'SPRING'
                WHEN MONTH(TO_DATE(CONCAT(sale_month, '-01'))) IN (6, 7, 8) THEN 'SUMMER'
                WHEN MONTH(TO_DATE(CONCAT(sale_month, '-01'))) IN (9, 10, 11) THEN 'AUTUMN'
                ELSE 'WINTER'
            END AS calendar_season
        FROM {task2_monthly_patterns}
    ),
    in_season AS (
        SELECT
            performance_group,
            SUM(
                CASE WHEN calendar_season = season THEN total_pieces_sold ELSE 0 END
            ) AS in_season_pieces,
            SUM(total_pieces_sold) AS all_pieces
        FROM monthly_with_season
        GROUP BY performance_group
    )
    SELECT
        t.performance_group,
        t.product_count,
        t.average_active_sale_months_per_product,
        t.share_of_products_selling_in_a_single_month,
        t.share_of_products_selling_in_all_sale_months,
        t.average_pieces_sold_per_active_month,
        ROUND(i.in_season_pieces / i.all_pieces, 4) AS in_season_share_of_group_pieces
    FROM timing t
        JOIN in_season i ON t.performance_group = i.performance_group
    ORDER BY t.performance_group
    """


def task2_month_share_deltas_sql(
    task2_monthly_patterns: str,
    top_group: str = TOP_GROUP_LABEL,
    bottom_group: str = BOTTOM_GROUP_LABEL,
) -> str:
    """Compare within-group monthly shares between the top 10% and bottom 10%
    sellers.

    One row per calendar sale month present in both groups: each group's share of
    its own lifetime pieces sold in that month (0-1), the share delta in
    percentage points (top minus bottom), and which group the month skews towards.
    Shares are computed within each group, so group size never distorts the
    comparison — a near-zero delta is the finding that sale timing does not
    separate the deciles. A thin MV over task2_monthly_patterns: it takes no input
    beyond that one upstream gold object.
    """
    return f"""
    WITH comparison AS (
        SELECT *
        FROM {task2_monthly_patterns}
        WHERE performance_group IN ('{top_group}', '{bottom_group}')
    ),
    group_month AS (
        SELECT sale_month, performance_group, SUM(total_pieces_sold) AS group_month_pieces
        FROM comparison
        GROUP BY sale_month, performance_group
    ),
    group_totals AS (
        SELECT performance_group, SUM(total_pieces_sold) AS group_total_pieces
        FROM comparison
        GROUP BY performance_group
    ),
    shares AS (
        SELECT
            gm.sale_month,
            gm.performance_group,
            gm.group_month_pieces / gt.group_total_pieces AS share_of_group
        FROM group_month gm
            JOIN group_totals gt ON gm.performance_group = gt.performance_group
    ),
    pivoted AS (
        SELECT
            sale_month,
            MAX(CASE WHEN performance_group = '{top_group}' THEN share_of_group END)
                AS top_10_percent_share_of_group,
            MAX(CASE WHEN performance_group = '{bottom_group}' THEN share_of_group END)
                AS bottom_10_percent_share_of_group
        FROM shares
        GROUP BY sale_month
    )
    SELECT
        sale_month,
        top_10_percent_share_of_group,
        bottom_10_percent_share_of_group,
        (top_10_percent_share_of_group - bottom_10_percent_share_of_group) * 100.0
            AS share_delta_percentage_points,
        CASE
            WHEN top_10_percent_share_of_group > bottom_10_percent_share_of_group
                THEN '{top_group}'
            WHEN top_10_percent_share_of_group < bottom_10_percent_share_of_group
                THEN '{bottom_group}'
            ELSE 'equal'
        END AS skews_towards
    FROM pivoted
    WHERE top_10_percent_share_of_group IS NOT NULL
        AND bottom_10_percent_share_of_group IS NOT NULL
    ORDER BY sale_month
    """


def task2_activity_breadth_by_group_sql(
    silver_sales: str, task2_product_performance: str
) -> str:
    """Cross-tabulate activity breadth (months with sales) against the seller
    groups.

    One row per performance group and breadth tier: the tier's product count and
    its share of the group's products (0-1; within a group the shares sum to 1).
    Tiers: 'no sales' (zero-filled catalog products), '1 month', '2 to 5 months',
    '6 to N-1 months', 'all N months' — N is the data window's distinct sale-month
    count, re-derived from silver.sales on every refresh via CONCAT/CAST rather
    than baked into this view's text, so the tier labels stay correct as the data
    window grows. The 'all N months' tier is matched before the range tiers, so a
    product active in every month of a short window (N <= 5) still lands in
    'all N months', not '2 to 5 months'.

    Motivation: the decile cut largely follows activity breadth, so this cross-tab
    shows how much of the attribute-delta signal is really a consistency signal —
    products with few active months concentrate in the bottom group, which makes
    the top-vs-bottom deltas a combined appeal-and-consistency comparison.
    """
    return f"""
    WITH per_product_months AS (
        SELECT product_id, COUNT(DISTINCT sale_month) AS active_sale_months
        FROM {silver_sales}
        GROUP BY product_id
    ),
    total_months AS (
        SELECT COUNT(DISTINCT sale_month) AS total_sale_months FROM {silver_sales}
    ),
    breadth AS (
        SELECT
            pp.product_id,
            pp.performance_group,
            COALESCE(ppm.active_sale_months, 0) AS active_sale_months,
            tm.total_sale_months
        FROM {task2_product_performance} pp
            LEFT JOIN per_product_months ppm ON pp.product_id = ppm.product_id
            CROSS JOIN total_months tm
    ),
    tiered AS (
        SELECT
            performance_group,
            CASE
                WHEN active_sale_months = 0 THEN 'no sales'
                WHEN active_sale_months = 1 THEN '1 month'
                WHEN active_sale_months = total_sale_months
                    THEN CONCAT('all ', CAST(total_sale_months AS STRING), ' months')
                WHEN active_sale_months <= 5 THEN '2 to 5 months'
                WHEN active_sale_months < total_sale_months
                    THEN CONCAT('6 to ', CAST(total_sale_months - 1 AS STRING), ' months')
                ELSE CONCAT('all ', CAST(total_sale_months AS STRING), ' months')
            END AS active_sale_months_tier
        FROM breadth
    ),
    counts AS (
        SELECT performance_group, active_sale_months_tier, COUNT(*) AS product_count
        FROM tiered
        GROUP BY performance_group, active_sale_months_tier
    ),
    group_sizes AS (
        SELECT performance_group, SUM(product_count) AS group_product_count
        FROM counts
        GROUP BY performance_group
    )
    SELECT
        c.performance_group,
        c.active_sale_months_tier,
        c.product_count,
        c.product_count / g.group_product_count AS share_of_group
    FROM counts c
        JOIN group_sizes g ON c.performance_group = g.performance_group
    ORDER BY c.performance_group, c.active_sale_months_tier
    """
