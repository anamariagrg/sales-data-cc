# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 — Silver Transform
# MAGIC Cast Bronze data, quarantine rejected rows, dedupe, enrich sales, and re-declare
# MAGIC Silver's Delta constraints.

# COMMAND ----------

# MAGIC %run ./common/config

# COMMAND ----------

# MAGIC %run ./common/quality_checks

# COMMAND ----------

# MAGIC %run ./common/silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.products

# COMMAND ----------

bronze_products = snake_case_columns(spark.table(table_name("bronze", "products_raw")))
tagged_products = cast_products(bronze_products)
valid_products, quarantine_products, deduped_products_count = split_products(tagged_products)

write_silver_table(valid_products, table_name("silver", "products"), ["product_id"], load_mode)
write_quarantine_table(quarantine_products, table_name("quarantine", "products"), load_mode)

print(
    f"products: valid={valid_products.count()}, "
    f"quarantined={quarantine_products.count()}, deduped={deduped_products_count} "
    f"(bronze={bronze_products.count()})"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.discounts
# MAGIC Remove exact copies and report their count; quarantine conflicting values for one key.
# MAGIC Rows whose `product_id` has no valid product are quarantined as `missing_product_reference`.

# COMMAND ----------

bronze_discounts = snake_case_columns(spark.table(table_name("bronze", "discounts_raw")))
tagged_discounts = cast_discounts(bronze_discounts)
valid_discounts, quarantine_discounts, deduped_discounts_count = split_discounts(
    tagged_discounts, reference_product_ids=valid_products.select("product_id")
)

write_silver_table(
    valid_discounts, table_name("silver", "discounts"), ["product_id", "city"], load_mode
)
write_quarantine_table(quarantine_discounts, table_name("quarantine", "discounts"), load_mode)

print(
    f"discounts: valid={valid_discounts.count()}, "
    f"quarantined={quarantine_discounts.count()}, deduped={deduped_discounts_count} "
    f"(bronze={bronze_discounts.count()})"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.sales
# MAGIC Enforce the assumed event key `(product_id, city, sale_ts)` before merging.
# MAGIC Rows whose `product_id` has no valid product are quarantined as `missing_product_reference`.

# COMMAND ----------

bronze_sales = snake_case_columns(spark.table(table_name("bronze", "sales_raw")))
tagged_sales = cast_sales(bronze_sales)
valid_sales, quarantine_sales, deduped_sales_count = split_sales(
    tagged_sales, reference_product_ids=valid_products.select("product_id")
)

write_silver_table(
    valid_sales, table_name("silver", "sales"), ["product_id", "city", "sale_ts"], load_mode
)
write_quarantine_table(quarantine_sales, table_name("quarantine", "sales"), load_mode)

print(
    f"sales: valid={valid_sales.count()}, quarantined={quarantine_sales.count()}, "
    f"deduped={deduped_sales_count} (bronze={bronze_sales.count()})"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.sales_enriched
# MAGIC Build row-level gross/net amounts from the valid DataFrames above. Row count is
# MAGIC checked against `valid_sales` before writing; a mismatch raises instead of merging.

# COMMAND ----------

silver_sales_enriched = build_sales_enriched(valid_sales, valid_products, valid_discounts)

sales_count = valid_sales.count()
sales_enriched_count = silver_sales_enriched.count()
if sales_enriched_count != sales_count:
    raise ValueError(
        f"sales_enriched row count ({sales_enriched_count}) != sales row count "
        f"({sales_count}); refusing to write sales_enriched."
    )

write_silver_table(
    silver_sales_enriched,
    table_name("silver", "sales_enriched"),
    ["product_id", "city", "sale_ts"],
    load_mode,
)

print(f"sales_enriched: {sales_enriched_count} rows (matches sales count)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Warnings — informational, never fail the run
# MAGIC Quarantine rule breakdown per entity, sales without a discount row (Gold coalesces
# MAGIC the missing discount to 0 by design), and catalog products never sold.

# COMMAND ----------

for entity, quarantine_df in (
    ("products", quarantine_products),
    ("discounts", quarantine_discounts),
    ("sales", quarantine_sales),
):
    total = quarantine_df.count()
    if total:
        print(f"[WARN] quarantine.{entity}: {total} row(s), {quarantine_rule_counts(quarantine_df)}")

sales_without_discount = anti_join_count(valid_sales, valid_discounts, ["product_id", "city"])
if sales_without_discount:
    print(
        f"[WARN] {sales_without_discount} distinct (product_id, city) pair(s) in sales "
        "have no discount row"
    )

zero_sales_products = anti_join_count(valid_products, valid_sales, ["product_id"])
if zero_sales_products:
    print(f"[WARN] {zero_sales_products} product_id(s) never sold")

# COMMAND ----------

# DBTITLE 1,Re-declare silver constraints
# overwriteSchema (initial mode) drops Delta constraints; re-declared here every
# run (idempotent — a no-op metadata check once already applied).
declare_silver_constraints(catalog)
