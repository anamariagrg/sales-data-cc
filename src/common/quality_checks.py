# Databricks notebook source
# MAGIC %md
# MAGIC ### common/quality_checks
# MAGIC Quarantine rules, row splitting, and shared DQ counters.
# MAGIC Loaded through `%run`; callers handle reporting and failure policy.

# COMMAND ----------

from typing import Iterable, List, Sequence, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine rule builders
# MAGIC Rules return null on pass or a rule name on failure; first failing rule wins.

# COMMAND ----------

def null_key_rule(key_columns: Sequence[str], rule_name: str = "null_key") -> Column:
    """Fail when any of key_columns is null."""
    condition = F.lit(False)
    for column in key_columns:
        condition = condition | F.col(column).isNull()
    return F.when(condition, F.lit(rule_name))


def null_or_unparseable_rule(raw_column: str, cast_column: Column, rule_name: str) -> Column:
    """Flag missing source values or failed casts."""
    return F.when(F.col(raw_column).isNull() | cast_column.isNull(), F.lit(rule_name))


def out_of_range_rule(column: Column, low: float, high: float, rule_name: str) -> Column:
    """Fail when column (already cast) is null or outside the inclusive [low, high] range."""
    return F.when(
        column.isNull() | (column < F.lit(low)) | (column > F.lit(high)), F.lit(rule_name)
    )


def non_positive_rule(column: Column, rule_name: str) -> Column:
    """Fail when column (already cast) is null or <= 0."""
    return F.when(column.isNull() | (column <= F.lit(0)), F.lit(rule_name))


def conflicting_duplicate_rule(
    df: DataFrame, key_columns: Sequence[str], rule_name: str = "conflicting_duplicate_key"
) -> Column:
    """Flag keys with differing business values, ignoring metadata and exact copies."""
    value_columns = [c for c in df.columns if c not in key_columns and not c.startswith("_")]
    window = Window.partitionBy(*key_columns)
    distinct_value_combinations = F.size(F.collect_set(F.struct(*value_columns)).over(window))
    return F.when(distinct_value_combinations > 1, F.lit(rule_name))


def split_valid_invalid(df: DataFrame, rule_columns: List[str]) -> Tuple[DataFrame, DataFrame]:
    """Return (valid, quarantine); rejects retain one `_dq_rule` label and input values."""
    combined = F.col(rule_columns[0]) if len(rule_columns) == 1 else F.coalesce(*rule_columns)
    tagged = df.withColumn("_dq_rule", combined)
    valid = tagged.filter(F.col("_dq_rule").isNull()).drop("_dq_rule", *rule_columns)
    quarantine = tagged.filter(F.col("_dq_rule").isNotNull()).drop(
        *[c for c in rule_columns if c != "_dq_rule"]
    )
    return valid, quarantine


def dedupe_and_count(df: DataFrame) -> Tuple[DataFrame, int]:
    """Return (distinct rows, removed-copy count) for reconciliation and reporting."""
    before = df.count()
    deduped = df.dropDuplicates()
    return deduped, before - deduped.count()


def split_missing_reference(
    valid: DataFrame,
    quarantine: DataFrame,
    reference: DataFrame,
    key_columns: Sequence[str],
    rule_name: str = "missing_product_reference",
) -> Tuple[DataFrame, DataFrame]:
    """Move valid rows whose key_columns have no match in the reference into quarantine.

    Runs after split_valid_invalid and before lineage is dropped, so moved rows keep
    their Bronze lineage and gain a `_dq_rule` label like any other reject. Keeps
    Silver reference sets (sales/discounts vs products) free of orphan keys, so the
    RELY foreign keys declared on the Silver tables stay true and the optimizer's
    join-elimination rewrites cannot over-count.
    """
    if reference is None:
        return valid, quarantine
    reference_keys = reference.select(*key_columns).distinct()
    orphan_keys = (
        valid.select(*key_columns).distinct().join(reference_keys, key_columns, "left_anti")
    )
    if orphan_keys.isEmpty():
        return valid, quarantine
    orphan_keys = F.broadcast(orphan_keys.withColumn("_is_orphan", F.lit(True)))
    marked = valid.join(orphan_keys, key_columns, "left")
    moved = (
        marked.filter(F.col("_is_orphan"))
        .drop("_is_orphan")
        .withColumn("_dq_rule", F.lit(rule_name))
    )
    kept = marked.filter(F.col("_is_orphan").isNull()).drop("_is_orphan")
    return kept, quarantine.unionByName(moved, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ query helpers
# MAGIC `anti_join_count`/`quarantine_rule_counts` back `02_silver_transform.py`'s warnings.

# COMMAND ----------

def anti_join_count(left: DataFrame, right: DataFrame, on: Iterable[str]) -> int:
    """Count distinct `on`-key rows in `left` that have no match in `right`."""
    on = list(on)
    return left.select(*on).distinct().join(right.select(*on).distinct(), on, "left_anti").count()


def quarantine_rule_counts(df: DataFrame) -> dict:
    """Count quarantined rows per `_dq_rule` value, e.g. {"price_null_or_unparseable": 3}."""
    rows = df.groupBy("_dq_rule").count().collect()
    return {row["_dq_rule"]: row["count"] for row in rows}