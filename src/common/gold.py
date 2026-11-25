# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ### common/gold
# MAGIC Business-level aggregations for Task 2, pulled in by
# MAGIC `03_gold_aggregations.py` via `%run`. Do not run this notebook standalone.

# COMMAND ----------

# DBTITLE 1,Business-level aggregations
from typing import Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _join_reference(
    left: DataFrame,
    right: DataFrame,
    on: Tuple[str, ...],
    how: str,
) -> DataFrame:
    """Join a reference DataFrame; relies on Spark's automatic broadcast."""
    return left.join(right, list(on), how)


def rank_product_performance(
    sales: DataFrame,
    products: DataFrame,
    group_fraction: float = 0.10,
) -> DataFrame:
    """Classify products into top/middle/bottom percentile groups by lifetime pieces sold.

    `percent_rank()` orders products by total pieces sold (0 = fewest pieces,
    1 = most pieces); the bottom group is rank <= group_fraction, the top group is
    rank >= 1 - group_fraction, and everything else is the middle comparison base.
    Tied products share a percentile value, so a tie spanning a cut-off pulls the
    whole tie into one group — deterministic, unlike NTILE, which would split
    boundary ties arbitrarily. Unsold catalog products are zero-filled and rank
    into the bottom group instead of being dropped from the comparison.
    """
    aggregations = [
        F.sum("pieces_sold").alias("total_pieces_sold"),
        F.countDistinct("city").alias("selling_city_count"),
    ]
    # Timestamp bounds are unconditional: silver.sales always derives sale_ts from epoch seconds.
    aggregations += [
        F.min("sale_ts").alias("first_sale_ts"),
        F.max("sale_ts").alias("last_sale_ts"),
    ]
    # Collapse to one row per product: lifetime pieces sold and selling-city breadth.
    totals = sales.groupBy("product_id").agg(*aggregations)
    # Left join attaches catalog attributes; unsold products have no row here yet.
    performance = _join_reference(
        totals, products, ("product_id",), "left"
    )
    # Re-add catalog products with zero sales, zero-filled, so they are ranked instead of dropped.
    unsold_products = products.join(
        totals.select("product_id"), "product_id", "left_anti"
    ).withColumns(
        {
            "total_pieces_sold": F.lit(0),
            "selling_city_count": F.lit(0),
            # unionByName needs matching columns: null-cast timestamp bounds onto unsold rows.
            "first_sale_ts": F.lit(None).cast(
                performance.schema["first_sale_ts"].dataType
            ),
            "last_sale_ts": F.lit(None).cast(
                performance.schema["last_sale_ts"].dataType
            ),
        }
    )
    # Append the unsold rows so every catalog product gets a group label.
    performance = performance.unionByName(
        unsold_products.select(*performance.columns)
    )

    # One window pass replaces the old max/min extremes: the catalog is small enough
    # that the single-partition window is cheap, and ties keep their shared rank.
    pieces_order = Window.orderBy(F.col("total_pieces_sold"))
    bottom_label = f"bottom {round(group_fraction * 100)}%"
    top_label = f"top {round(group_fraction * 100)}%"
    middle_label = f"middle {round((1 - 2 * group_fraction) * 100)}%"
    return (
        performance.withColumn("percentile_rank", F.percent_rank().over(pieces_order))
        .withColumn(
            "performance_group",
            F.when(
                F.col("percentile_rank") <= F.lit(group_fraction), F.lit(bottom_label)
            )
            .when(
                F.col("percentile_rank") >= F.lit(1 - group_fraction),
                F.lit(top_label),
            )
            .otherwise(F.lit(middle_label)),
        )
    )

# COMMAND ----------

# DBTITLE 1,Task 2 summaries
# MAGIC %md
# MAGIC ## Task 2 summaries
# MAGIC Products are grouped into top 10% / middle 80% / bottom 10% by lifetime pieces sold
# MAGIC (see `rank_product_performance`). Mix comparisons use within-group percentages so
# MAGIC different group sizes never distort them; the attribute table stores the top-vs-bottom
# MAGIC share delta in percentage points. Monthly sales retain the product's catalog season,
# MAGIC separate from the calendar month when the sale occurred. Timing analyses extend the
# MAGIC monthly view with per-product sales consistency (months with sales), within-group
# MAGIC monthly share deltas, and an activity-breadth cross-tab against the seller groups.

# COMMAND ----------

# DBTITLE 1,Task 2 summaries
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


def product_performance_summary(performance: DataFrame) -> DataFrame:
    """Summarize product counts, pieces sold, and catalog list price by seller group."""
    return (
        performance.groupBy("performance_group")
        .agg(
            F.countDistinct("product_id").alias("product_count"),
            F.sum("total_pieces_sold").alias("total_pieces_sold"),
            F.avg("total_pieces_sold").alias("average_pieces_per_product"),
            F.min("total_pieces_sold").alias("minimum_pieces_per_product"),
            F.max("total_pieces_sold").alias("maximum_pieces_per_product"),
            F.avg("price").alias("average_catalog_list_price"),
            F.percentile_approx("price", 0.5).alias("median_catalog_list_price"),
        )
        .orderBy("performance_group")
    )


def product_top_10_vs_bottom_10_attribute_deltas(
    performance: DataFrame,
) -> DataFrame:
    """Compare within-group attribute shares between the top 10% and bottom 10% sellers.

    Every varying catalog attribute value gets one row: the number of top-group and
    bottom-group products carrying that value, each group's share of its own products
    (0-1), the share delta in percentage points (top share minus bottom share), and
    which group the value skews towards. Shares are computed within each group, so the
    two group sizes never distort the comparison; a value absent from one group is a
    genuine 0% share of that group.

    All attribute values are kept, including barely-separating ones — "no signal"
    (e.g. promotion) is a finding, not noise. Presentation layers select strong
    patterns by filtering ABS(share_delta_percentage_points) (e.g. >= 1 percentage
    point). The middle 80% is the comparison base in task2_performance_summary only.
    """
    # Group labels produced by rank_product_performance's default grouping.
    top_group = "top 10%"
    bottom_group = "bottom 10%"
    comparison = performance.filter(
        F.col("performance_group").isin(top_group, bottom_group)
    )
    group_sizes = comparison.groupBy("performance_group").agg(
        F.countDistinct("product_id").alias("group_product_count")
    )
    attribute_frames = []
    for attribute in PRODUCT_CATEGORICAL_ATTRIBUTES:
        normalized_value = F.trim(F.col(attribute).cast("string"))
        attribute_frames.append(
            comparison.select(
                "product_id",
                "performance_group",
                F.lit(attribute).alias("attribute_name"),
                F.when(
                    normalized_value.isNull() | (normalized_value == ""),
                    F.lit("Unknown"),
                )
                .otherwise(normalized_value)
                .alias("attribute_value"),
            )
        )

    attribute_rows = attribute_frames[0]
    for frame in attribute_frames[1:]:
        attribute_rows = attribute_rows.unionByName(frame)

    counts = attribute_rows.groupBy(
        "performance_group", "attribute_name", "attribute_value"
    ).agg(F.countDistinct("product_id").alias("product_count"))
    # Within-group share: value count divided by that group's product count.
    shares = (
        counts.join(group_sizes, "performance_group", "inner").withColumn(
            "group_share", F.col("product_count") / F.col("group_product_count")
        )
    )
    # One row per attribute value: both groups' counts and shares side by side.
    # coalesce turns "value never appears in this group" into a real zero share.
    return (
        shares.groupBy("attribute_name", "attribute_value")
        .agg(
            F.coalesce(
                F.max(
                    F.when(
                        F.col("performance_group") == F.lit(top_group),
                        F.col("product_count"),
                    )
                ),
                F.lit(0),
            ).alias("top_10_percent_product_count"),
            F.coalesce(
                F.max(
                    F.when(
                        F.col("performance_group") == F.lit(top_group),
                        F.col("group_share"),
                    )
                ),
                F.lit(0.0),
            ).alias("top_10_percent_share_of_group"),
            F.coalesce(
                F.max(
                    F.when(
                        F.col("performance_group") == F.lit(bottom_group),
                        F.col("product_count"),
                    )
                ),
                F.lit(0),
            ).alias("bottom_10_percent_product_count"),
            F.coalesce(
                F.max(
                    F.when(
                        F.col("performance_group") == F.lit(bottom_group),
                        F.col("group_share"),
                    )
                ),
                F.lit(0.0),
            ).alias("bottom_10_percent_share_of_group"),
        )
        .withColumn(
            "share_delta_percentage_points",
            (F.col("top_10_percent_share_of_group")
             - F.col("bottom_10_percent_share_of_group"))
            * F.lit(100.0),
        )
        .withColumn(
            "skews_towards",
            F.when(
                F.col("share_delta_percentage_points") >= 0, F.lit(top_group)
            ).otherwise(F.lit(bottom_group)),
        )
        .orderBy(
            F.desc(F.abs("share_delta_percentage_points")),
            "attribute_name",
            "attribute_value",
        )
    )


def monthly_sales_patterns(sales: DataFrame, product_performance: DataFrame) -> DataFrame:
    """Monthly pieces and share by calendar month, product season, and seller group."""
    classified = sales.join(
        product_performance.select("product_id", "performance_group", "season"),
        "product_id", "left",
    )
    # Reuse Silver's calendar month rather than deriving it again.
    monthly = (
        classified.groupBy("sale_month", "season", "performance_group")
        .agg(
            F.sum("pieces_sold").alias("total_pieces_sold"),
            F.countDistinct("product_id").alias("product_count"),
        )
    )
    season_group_window = Window.partitionBy("season", "performance_group")
    return (
        monthly.withColumn(
            "group_season_total_pieces",
            F.sum("total_pieces_sold").over(season_group_window),
        )
        .withColumn(
            "group_season_pieces_pct",
            F.col("total_pieces_sold")
            * F.lit(100.0)
            / F.col("group_season_total_pieces"),
        )
        .orderBy("sale_month", "season", "performance_group")
    )


def product_sales_timing_summary(
    sales: DataFrame,
    product_performance: DataFrame,
    monthly_patterns: DataFrame,
) -> DataFrame:
    """Per-group sales-timing metrics: consistency, intensity, and in-season share.

    Answers the sales-month half of Task 2 with three complementary metrics.
    (1) Consistency: how many calendar months each product sells in — the deciles'
    strongest timing separator, because total pieces scale with months on sale.
    (2) Intensity: average pieces sold per active month, which separates scale from
    breadth. (3) In-season alignment: the share of a group's pieces sold in calendar
    months matching the product's catalog season, computed from monthly_patterns
    (sale month and catalog season are independent dimensions; the data window has
    no December, so winter-catalog products only meet January and February).

    The data window's number of distinct sale months is derived from the sales
    input at each rebuild, so share_of_products_selling_in_all_sale_months stays
    correct if the window gains or loses months. Products with
    no sales (zero-filled in performance) enter with 0 active months and contribute
    0 to the single-month and all-months shares; the pieces-per-month average only
    covers products with at least one active month. Shares are 0-1; derived
    averages are rounded to 2 decimals, shares to 4.
    """
    per_product_months = sales.groupBy("product_id").agg(
        F.countDistinct("sale_month").alias("active_sale_months"),
        F.sum("pieces_sold").alias("product_total_pieces_sold"),
    )
    # Left join keeps zero-filled catalog products; they have no silver sales rows.
    breadth = (
        product_performance.select("product_id", "performance_group")
        .join(per_product_months, "product_id", "left")
        .withColumns(
            {
                "active_sale_months": F.coalesce(F.col("active_sale_months"), F.lit(0)),
                "product_total_pieces_sold": F.coalesce(
                    F.col("product_total_pieces_sold"), F.lit(0)
                ),
            }
        )
    )
    # The data window's distinct sale-month count, derived from the sales input
    # (never hard-coded): the all-months share is defined against this.
    total_sale_months = sales.select("sale_month").distinct().count()

    timing = breadth.groupBy("performance_group").agg(
        F.count("*").alias("product_count"),
        F.round(F.avg("active_sale_months"), 2).alias(
            "average_active_sale_months_per_product"
        ),
        F.round(
            F.avg(
                F.when(F.col("active_sale_months") == F.lit(1), F.lit(1.0)).otherwise(
                    F.lit(0.0)
                )
            ),
            4,
        ).alias("share_of_products_selling_in_a_single_month"),
        F.round(
            F.avg(
                F.when(
                    F.col("active_sale_months") == F.lit(total_sale_months),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
            ),
            4,
        ).alias("share_of_products_selling_in_all_sale_months"),
        F.round(
            F.avg(
                F.when(
                    F.col("active_sale_months") > F.lit(0),
                    F.col("product_total_pieces_sold") / F.col("active_sale_months"),
                )
            ),
            2,
        ).alias("average_pieces_sold_per_active_month"),
    )
    # Month number of the 'YYYY-MM' sale_month key decides the calendar season.
    month_number = F.month(F.to_date(F.concat(F.col("sale_month"), F.lit("-01"))))
    calendar_season = (
        F.when(month_number.isin(3, 4, 5), F.lit("SPRING"))
        .when(month_number.isin(6, 7, 8), F.lit("SUMMER"))
        .when(month_number.isin(9, 10, 11), F.lit("AUTUMN"))
        .otherwise(F.lit("WINTER"))
    )
    in_season = (
        monthly_patterns.withColumn("calendar_season", calendar_season)
        .groupBy("performance_group")
        .agg(
            F.sum(
                F.when(
                    F.col("calendar_season") == F.col("season"),
                    F.col("total_pieces_sold"),
                ).otherwise(F.lit(0))
            ).alias("in_season_pieces"),
            F.sum("total_pieces_sold").alias("all_pieces"),
        )
        .withColumn(
            "in_season_share_of_group_pieces",
            F.round(F.col("in_season_pieces") / F.col("all_pieces"), 4),
        )
        .select("performance_group", "in_season_share_of_group_pieces")
    )
    return timing.join(in_season, "performance_group", "inner").orderBy(
        "performance_group"
    )


def product_month_share_deltas(monthly_patterns: DataFrame) -> DataFrame:
    """Compare within-group monthly shares between the top 10% and bottom 10% sellers.

    One row per calendar sale month: each group's share of its own lifetime pieces
    sold in that month (0-1), the share delta in percentage points (top minus
    bottom), and which group the month skews towards. Shares are computed within
    each group, so group size never distorts the comparison — a near-zero delta is
    the finding that sale timing does not separate the deciles.
    """
    # Group labels produced by rank_product_performance's default grouping.
    top_group = "top 10%"
    bottom_group = "bottom 10%"
    comparison = monthly_patterns.filter(
        F.col("performance_group").isin(top_group, bottom_group)
    )
    group_month = comparison.groupBy("sale_month", "performance_group").agg(
        F.sum("total_pieces_sold").alias("group_month_pieces")
    )
    group_totals = comparison.groupBy("performance_group").agg(
        F.sum("total_pieces_sold").alias("group_total_pieces")
    )
    shares = (
        group_month.join(group_totals, "performance_group", "inner")
        .withColumn(
            "share_of_group",
            F.col("group_month_pieces") / F.col("group_total_pieces"),
        )
    )
    top_shares = shares.filter(F.col("performance_group") == F.lit(top_group)).select(
        "sale_month",
        F.col("share_of_group").alias("top_10_percent_share_of_group"),
    )
    bottom_shares = shares.filter(
        F.col("performance_group") == F.lit(bottom_group)
    ).select(
        "sale_month",
        F.col("share_of_group").alias("bottom_10_percent_share_of_group"),
    )
    return (
        top_shares.join(bottom_shares, "sale_month", "inner")
        .withColumn(
            "share_delta_percentage_points",
            (F.col("top_10_percent_share_of_group")
             - F.col("bottom_10_percent_share_of_group"))
            * F.lit(100.0),
        )
        .withColumn(
            "skews_towards",
            F.when(F.col("share_delta_percentage_points") > 0, F.lit(top_group))
            .when(F.col("share_delta_percentage_points") < 0, F.lit(bottom_group))
            .otherwise(F.lit("equal")),
        )
        .orderBy("sale_month")
    )


def activity_breadth_by_performance_group(
    sales: DataFrame,
    product_performance: DataFrame,
) -> DataFrame:
    """Cross-tabulate activity breadth (months with sales) against the seller groups.

    One row per performance group and breadth tier: the tier's product count and its
    share of the group's products (0-1; within a group the shares sum to 1). Tiers:
    'no sales' (zero-filled catalog products), '1 month', '2 to 5 months', '6 to N-1
    months', 'all N months' — N is the data window's distinct sale-month count,
    derived from the sales input at each rebuild (e.g. 'all 11 months' for an
    11-month window). The 'all N months' tier is matched before the range tiers, so
    a product active in every month of a short window (N <= 5) still lands in
    'all N months', not '2 to 5 months'.

    Motivation: the decile cut largely follows activity breadth, so this cross-tab
    shows how much of the attribute-delta signal is really a consistency signal —
    products with few active months concentrate in the bottom group, which makes
    the top-vs-bottom deltas a combined appeal-and-consistency comparison.
    """
    per_product_months = sales.groupBy("product_id").agg(
        F.countDistinct("sale_month").alias("active_sale_months")
    )
    # The data window's distinct sale-month count, derived from the sales input
    # (never hard-coded): the tiers above '2 to 5 months' are labelled from it.
    total_sale_months = sales.select("sale_month").distinct().count()
    breadth = (
        product_performance.select("product_id", "performance_group")
        .join(per_product_months, "product_id", "left")
        .withColumn(
            "active_sale_months", F.coalesce(F.col("active_sale_months"), F.lit(0))
        )
    )
    # 'all N months' is matched before the range tiers so a product active in every
    # month of a short window (N <= 5) still lands in the all-months tier.
    breadth_tier = (
        F.when(F.col("active_sale_months") == F.lit(0), F.lit("no sales"))
        .when(F.col("active_sale_months") == F.lit(1), F.lit("1 month"))
        .when(
            F.col("active_sale_months") == F.lit(total_sale_months),
            F.lit(f"all {total_sale_months} months"),
        )
        .when(F.col("active_sale_months") <= F.lit(5), F.lit("2 to 5 months"))
        .when(
            F.col("active_sale_months") < F.lit(total_sale_months),
            F.lit(f"6 to {total_sale_months - 1} months"),
        )
        .otherwise(F.lit(f"all {total_sale_months} months"))
    )
    counts = (
        breadth.withColumn("active_sale_months_tier", breadth_tier)
        .groupBy("performance_group", "active_sale_months_tier")
        .agg(F.count("*").alias("product_count"))
    )
    group_sizes = counts.groupBy("performance_group").agg(
        F.sum("product_count").alias("group_product_count")
    )
    return (
        counts.join(group_sizes, "performance_group", "inner")
        .withColumn(
            "share_of_group", F.col("product_count") / F.col("group_product_count")
        )
        .orderBy("performance_group", "active_sale_months_tier")
    )


def write_gold_table(
    df: DataFrame,
    full_table_name: str,
    partition_columns: Optional[Tuple[str, ...]] = None,
    comment: Optional[str] = None,
) -> None:
    """Overwrite a gold table — always rebuilt from canonical silver inputs."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.saveAsTable(full_table_name)
    if comment:
        # Re-applied after every rebuild so the description survives overwrites.
        escaped_comment = comment.replace("'", "''")
        spark.sql(
            f"COMMENT ON TABLE {full_table_name} IS '{escaped_comment}'"
        )