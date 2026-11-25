# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Module doc
# MAGIC %md
# MAGIC ### common/silver
# MAGIC Casting, quarantine splits, enrichment, and Delta writes, loaded through `%run`.
# MAGIC Each cast function first runs a fail-fast schema check (`verify_columns`) against the full expected Bronze business schema, so passthrough columns Gold consumes are covered too.
# MAGIC Valid rows drop Bronze lineage; rejected rows retain it. exact copies are removed during the split (same as discounts/sales) so they cannot cause a Delta MERGE "multiple source rows matched" error.
# MAGIC Discounts and sales whose `product_id` has no match in this run's valid products are quarantined as `missing_product_reference` (orphan-key gate), keeping the Silver RELY foreign keys true.
# MAGIC

# COMMAND ----------

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType

try:
    from .quality_checks import (
        conflicting_duplicate_rule,
        dedupe_and_count,
        non_positive_rule,
        null_key_rule,
        null_or_unparseable_rule,
        out_of_range_rule,
        split_missing_reference,
        split_valid_invalid,
    )
except ImportError:
    pass  # %run loads common/quality_checks into the caller's namespace instead.

# `_rescued_data` (added by Auto Loader for schema-mismatched rows) is treated
# like the other lineage columns here.
LINEAGE_COLUMNS = ["_ingested_at", "_source_file", "_ingestion_mode", "_rescued_data"]

# COMMAND ----------

def snake_case_columns(df: DataFrame) -> DataFrame:
    """Normalize source headers to lowercase snake_case."""
    normalized = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df.toDF(*normalized)

# COMMAND ----------

# DBTITLE 1,Schema check helper
# Full expected Bronze business schemas (_-prefixed lineage columns may also be present).
# Products lists every raw column — Gold consumes passthrough attributes (Task 2's
# seasonal/terms/material/product_position/promotion/origin, plus price/section/season)
# that the cast itself never touches. Discounts and sales list all their business columns.
REQUIRED_COLUMNS = {
    "products": [
        "product_id",
        "product_position",
        "promotion",
        "product_category",
        "seasonal",
        "brand",
        "url",
        "name",
        "description",
        "price",
        "currency",
        "terms",
        "section",
        "season",
        "material",
        "origin",
    ],
    "discounts": ["product_id", "city", "discount"],
    "sales": ["product_id", "city", "time", "pieces_sold"],
}


def verify_columns(df: DataFrame, dataset: str) -> None:
    """Fail fast when Bronze is missing any expected business column.

    Silver passes Bronze columns through untouched, and Gold consumes that full
    schema — so this checks the complete contract, not just cast inputs.
    """
    missing = [column for column in REQUIRED_COLUMNS[dataset] if column not in df.columns]
    if missing:
        raise ValueError(
            f"'{dataset}' bronze schema is missing required column(s) {missing}; "
            f"found {sorted(df.columns)}. Fix the source schema before transforming."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.products
# MAGIC Cast price and normalize section/season. Reject null keys, invalid prices, and conflicts.
# MAGIC Product keys are assumed unique; exact copies are not deduplicated here.

# COMMAND ----------

def cast_products(df: DataFrame) -> DataFrame:
    """Cast price safely and normalize attributes before checking business-key conflicts."""
    verify_columns(df, "products")
    price_cast = F.col("price").try_cast(DecimalType(10, 2))
    rule_price = null_or_unparseable_rule("price", price_cast, "price_null_or_unparseable")
    df = (
        df.withColumn("price", price_cast)
        .withColumn("section", F.upper(F.trim(F.col("section"))))
        .withColumn("season", F.upper(F.trim(F.col("season"))))
    )
    rule_key = null_key_rule(["product_id"])
    rule_conflict = conflicting_duplicate_rule(df, ["product_id"])
    return df.withColumn("_dq_rule_products", F.coalesce(rule_key, rule_price, rule_conflict))


def split_products(tagged: DataFrame):
    """Return (deduplicated valid rows, quarantine, removed-copy count)."""
    valid, quarantine = split_valid_invalid(tagged, ["_dq_rule_products"])
    valid = valid.drop(*LINEAGE_COLUMNS)
    valid_deduped, deduped_count = dedupe_and_count(valid)
    return valid_deduped, quarantine, deduped_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.discounts
# MAGIC Discounts must be fractions in `[0, 1]`; no percentage rescaling.
# MAGIC Normalize cities before conflict checks and remove exact copies during the split.
# MAGIC Rows whose `product_id` has no valid product are quarantined as `missing_product_reference`.

# COMMAND ----------

def cast_discounts(df: DataFrame) -> DataFrame:
    """Cast discount safely and normalize city before range/key/conflict checks."""
    verify_columns(df, "discounts")
    discount_cast = F.col("discount").try_cast(DecimalType(4, 3))
    df = df.withColumn("discount", discount_cast).withColumn(
        "city", F.upper(F.trim(F.col("city")))
    )
    rule_key = null_key_rule(["product_id", "city"])
    rule_range = out_of_range_rule(F.col("discount"), 0.0, 1.0, "discount_out_of_range_or_null")
    rule_conflict = conflicting_duplicate_rule(df, ["product_id", "city"])
    return df.withColumn("_dq_rule_discounts", F.coalesce(rule_key, rule_range, rule_conflict))


def split_discounts(tagged: DataFrame, reference_product_ids: DataFrame = None):
    """Return (deduplicated valid rows, quarantine, removed-copy count).

    Rows whose product_id has no match in reference_product_ids (this run's valid
    products) are quarantined as missing_product_reference, so silver.discounts
    never violates the discounts_products_fk RELY constraint.
    """
    valid, quarantine = split_valid_invalid(tagged, ["_dq_rule_discounts"])
    valid, quarantine = split_missing_reference(
        valid, quarantine, reference_product_ids, ["product_id"]
    )
    valid = valid.drop(*LINEAGE_COLUMNS)
    valid_deduped, deduped_count = dedupe_and_count(valid)
    return valid_deduped, quarantine, deduped_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.sales
# MAGIC Convert epoch seconds to `sale_ts`; derive dates/months in the session timezone (use UTC).
# MAGIC Reject null keys, invalid time/quantity, and conflicts on `(product_id, city, sale_ts)`.
# MAGIC Rows whose `product_id` has no valid product are quarantined as `missing_product_reference`.

# COMMAND ----------

def cast_sales(df: DataFrame) -> DataFrame:
    """Safely cast quantity/time, normalize city, and derive sale date/month."""
    verify_columns(df, "sales")
    time_long = F.col("time").try_cast(LongType())
    pieces_cast = F.col("pieces_sold").try_cast(IntegerType())
    rule_key = null_key_rule(["product_id", "city"])
    rule_sale_ts = null_or_unparseable_rule("time", time_long, "sale_ts_null_or_unparseable")
    rule_pieces = non_positive_rule(pieces_cast, "pieces_sold_null_or_non_positive")
    df = (
        df.withColumn(
            "_dq_rule_sales", F.coalesce(rule_key, rule_sale_ts, rule_pieces)
        )
        .withColumn("city", F.upper(F.trim(F.col("city"))))
        .withColumn("pieces_sold", pieces_cast)
        .withColumn("sale_ts", F.timestamp_seconds(time_long))
        .drop("time")
    )
    return df.withColumn("sale_date", F.to_date("sale_ts")).withColumn(
        "sale_month", F.date_format("sale_ts", "yyyy-MM")
    )


def split_sales(tagged: DataFrame, reference_product_ids: DataFrame = None):
    """Reject merge-key conflicts; return (deduplicated valid rows, quarantine, copy count).

    Rows whose product_id has no match in reference_product_ids (this run's valid
    products) are quarantined as missing_product_reference, so silver.sales never
    violates the sales_products_fk RELY constraint.
    """
    sales_key = ["product_id", "city", "sale_ts"]
    conflict = conflicting_duplicate_rule(tagged, sales_key)
    tagged = tagged.withColumn(
        "_dq_rule_sales", F.coalesce(F.col("_dq_rule_sales"), conflict)
    )
    valid, quarantine = split_valid_invalid(tagged, ["_dq_rule_sales"])
    valid, quarantine = split_missing_reference(
        valid, quarantine, reference_product_ids, ["product_id"]
    )
    valid = valid.drop(*LINEAGE_COLUMNS)
    valid_deduped, deduped_count = dedupe_and_count(valid)
    return valid_deduped, quarantine, deduped_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.sales_enriched
# MAGIC Left-join valid sales to products and discounts, then derive gross/net amounts.
# MAGIC This remains row-level Silver data; DQ checks that enrichment preserves the sales count.

# COMMAND ----------

def build_sales_enriched(sales: DataFrame, products: DataFrame, discounts: DataFrame) -> DataFrame:
    """Join valid sales to valid product + discount reference data; compute amount columns."""
    product_columns = [c for c in products.columns if not c.startswith("_")]
    # Unique reference keys prevent multiplication; left joins retain unmatched sales.
    enriched = sales.join(
        products.select(*product_columns), "product_id", "left"
    ).join(
        discounts.select("product_id", "city", "discount"), ["product_id", "city"], "left"
    )
    return (
        enriched.withColumn("discount_pct", F.coalesce(F.col("discount"), F.lit(0.0)))
        .withColumn("gross_amount", F.col("pieces_sold") * F.col("price"))
        .withColumn("net_amount", F.col("gross_amount") * (F.lit(1.0) - F.col("discount_pct")))
    )

# COMMAND ----------

# DBTITLE 1,Delta writes
def write_silver_table(
    df: DataFrame,
    full_table_name: str,
    key_columns: Iterable[str],
    load_mode: str,
) -> None:
    """Overwrite on initial creation; otherwise upsert delta rows by natural key."""
    if load_mode == "initial" or not df.sparkSession.catalog.tableExists(full_table_name):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_table_name)
        )
        return
    if load_mode != "delta":
        raise ValueError(f"Unknown load_mode '{load_mode}'; expected 'initial' or 'delta'.")

    # Schema evolution via the session config is rejected on serverless
    # (CONFIG_NOT_AVAILABLE), so use the per-statement SQL syntax instead.
    condition = " AND ".join(f"target.`{c}` = source.`{c}`" for c in key_columns)
    df.createOrReplaceTempView("merge_source")
    df.sparkSession.sql(
        f"""
        MERGE WITH SCHEMA EVOLUTION INTO {full_table_name} AS target
        USING merge_source AS source
        ON {condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def write_quarantine_table(df: DataFrame, full_table_name: str, load_mode: str) -> None:
    """Overwrite rejects on initial load; append history on delta without relying on a key."""
    write_mode = "overwrite" if load_mode == "initial" else "append"
    (
        df.write.format("delta")
        .mode(write_mode)
        .option("mergeSchema", "true")
        .saveAsTable(full_table_name)
    )

# COMMAND ----------

# DBTITLE 1,Silver PK/FK constraints (RELY)
def declare_silver_constraints(catalog: str) -> None:
    """Re-apply Silver constraints after each write (idempotent; overwriteSchema drops them).

    PK/FK are RELY-only (Delta/UC never validates them). Discount range, positive
    pieces_sold, and the listed NOT NULL columns are real, Delta-enforced constraints.
    """
    silver = f"{catalog}.silver"
    existing = {
        row["constraint_name"]
        for row in spark.sql(
            "SELECT constraint_name FROM system.information_schema.table_constraints "
            f"WHERE table_catalog = '{catalog}' AND table_schema = 'silver'"
        ).collect()
    }

    def set_not_null(table: str, column: str) -> None:
        is_nullable = spark.sql(
            "SELECT is_nullable FROM system.information_schema.columns "
            f"WHERE table_catalog = '{catalog}' AND table_schema = 'silver' "
            f"AND table_name = '{table}' AND column_name = '{column}'"
        ).collect()
        if is_nullable and is_nullable[0]["is_nullable"] == "YES":
            spark.sql(f"ALTER TABLE {silver}.{table} ALTER COLUMN {column} SET NOT NULL")

    def check_constraint_exists(table: str, name: str) -> bool:
        """CHECK constraints are Delta table properties, not UC catalog metadata —
        system.information_schema.table_constraints only tracks PK/FK, so existence
        must be checked via SHOW TBLPROPERTIES instead."""
        props = spark.sql(f"SHOW TBLPROPERTIES {silver}.{table}").collect()
        return any(row["key"] == f"delta.constraints.{name}" for row in props)

    if "products_pk" not in existing:
        set_not_null("products", "product_id")
        spark.sql(
            f"ALTER TABLE {silver}.products ADD CONSTRAINT products_pk "
            "PRIMARY KEY (product_id) RELY"
        )
    if "sales_products_fk" not in existing:
        spark.sql(
            f"ALTER TABLE {silver}.sales ADD CONSTRAINT sales_products_fk "
            f"FOREIGN KEY (product_id) REFERENCES {silver}.products RELY"
        )
    if "discounts_products_fk" not in existing:
        spark.sql(
            f"ALTER TABLE {silver}.discounts ADD CONSTRAINT discounts_products_fk "
            f"FOREIGN KEY (product_id) REFERENCES {silver}.products RELY"
        )

    # Enforced (non-RELY) rules; gated on the CHECK constraint so delta-mode
    # runs skip these once applied.
    if not check_constraint_exists("discounts", "discounts_discount_range"):
        set_not_null("discounts", "product_id")
        set_not_null("discounts", "city")
        set_not_null("discounts", "discount")
        spark.sql(
            f"ALTER TABLE {silver}.discounts ADD CONSTRAINT discounts_discount_range "
            "CHECK (discount >= 0 AND discount <= 1)"
        )

    if not check_constraint_exists("sales", "sales_pieces_sold_positive"):
        set_not_null("sales", "product_id")
        set_not_null("sales", "city")
        set_not_null("sales", "sale_ts")
        set_not_null("sales", "pieces_sold")
        spark.sql(
            f"ALTER TABLE {silver}.sales ADD CONSTRAINT sales_pieces_sold_positive "
            "CHECK (pieces_sold > 0)"
        )