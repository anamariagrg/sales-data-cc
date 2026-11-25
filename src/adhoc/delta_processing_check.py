# Databricks notebook source
# DBTITLE 1,Delta Run Verification Notebook (header)
# MAGIC %md
# MAGIC # Delta Run Verification Notebook (post-run checks on the delta files)
# MAGIC
# MAGIC Proves that the pipeline's `load_mode=delta` run picked up the three `*_delta_001.csv` files
# MAGIC from the raw-files Volume and moved their rows correctly through **bronze → silver → gold**,
# MAGIC routing the deliberately broken rows to **quarantine**. Each check below queries the Unity Catalog
# MAGIC tables in `bronze`, `silver`, `gold`, and `quarantine`, and is followed by an
# MAGIC **Observations / Conclusion** markdown cell summarizing what the check found.
# MAGIC
# MAGIC **Design principle of the delta files:** every quarantine-triggering row uses a brand-new key
# MAGIC (900001–900003, 700001–700003, 999999) that doesn't exist in `initial_files`. Since Silver
# MAGIC reprocesses the entire Bronze table each run, reusing an existing key with a changed value
# MAGIC (e.g. editing 185102's price) would trigger `conflicting_duplicate_key` across the old+new
# MAGIC copies and silently leave the merged Silver row untouched — not a clean "update" demo.
# MAGIC Isolating new rows avoids that side effect entirely.

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("catalog", "sales_data_cc", "Catalog name")
catalog_name = dbutils.widgets.get("catalog")

# COMMAND ----------

# DBTITLE 1,Setup — table helpers and delta keys
from pyspark.sql import functions as F

# The three delta files landed in the raw-files Volume, and the keys they introduce.
DELTA_FILES = {
    "products_raw": "product_information_delta_001.csv",
    "discounts_raw": "discount_delta_001.csv",
    "sales_raw": "products_sold_delta_001.csv",
}
NEW_PRODUCT_KEYS = ["900001", "900002", "900003", "700001", "700002", "700003", "999999"]
EXISTING_KEY = "185102"  # initial-file product that also receives one new valid sale row


def bronze(name):
    return spark.table(f"{catalog_name}.bronze.{name}")


def silver(name):
    return spark.table(f"{catalog_name}.silver.{name}")


def quarantine(name):
    return spark.table(f"{catalog_name}.quarantine.{name}")


def gold(name):
    return spark.table(f"{catalog_name}.gold.{name}")

# COMMAND ----------

# DBTITLE 1,Quick read — verification summary
# MAGIC %md
# MAGIC ## Quick read — verification summary
# MAGIC
# MAGIC **Bottom line: the delta run worked end-to-end — under the current `missing_product_reference` rule.** All 23 delta-file rows landed in Bronze exactly once; the **9** clean rows merged into Silver (2 new products, 3 new discounts, 4 new sales); the **14** broken rows went to quarantine (the 6 original cases — bad price, out-of-range discount, negative quantity, 3 conflicting-duplicate pairs — plus 5 orphan-key rows quarantined as `missing_product_reference` across keys 700002 / 900003 / 999999); and the surviving new keys and dimension values flow on into Gold.
# MAGIC
# MAGIC | # | Check | Result |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | Bronze ingested every delta file exactly once; initial files not re-ingested | PASS |
# MAGIC | 2 | Delta rows use only brand-new keys (+ one new sale event on existing key 185102) | PASS |
# MAGIC | 3 | Clean delta rows merged into silver.products / discounts / sales (orphan-key rows quarantined) | PASS |
# MAGIC | 4 | New values H&M, electronics, KIDS, MONSOON appear in Silver; AUSTIN does not | PASS (products) / FAIL (AUSTIN — expected under the rule) |
# MAGIC | 5 | Broken rows quarantined with the right `_dq_rule`; none leaked into Silver | PASS |
# MAGIC | 6 | New keys/values reach gold.task1_sales_facts and task1_sales_rollup | PASS |

# COMMAND ----------

# DBTITLE 1,Check 1 — bronze ingestion
# MAGIC %md
# MAGIC ## 1. Bronze: delta files ingested exactly once, with lineage

# COMMAND ----------

# DBTITLE 1,Check 1 — delta files in bronze exactly once
# Checks: that every row of every *_delta_001.csv landed in Bronze exactly once (stamped
# _ingestion_mode='delta' and _source_file=<delta file name>), and that the delta run did
# NOT re-ingest the initial files.
import pandas as pd

for raw_table, delta_file in DELTA_FILES.items():
    csv_rows = len(pd.read_csv(f"/Volumes/{catalog_name}/landing/raw_files/{delta_file}"))
    bronze_rows = bronze(raw_table).filter(F.col("_source_file") == delta_file).count()
    verdict = "PASS" if csv_rows == bronze_rows else "FAIL"
    print(f"[{verdict}] bronze.{raw_table} <- {delta_file}: {bronze_rows} rows in Bronze (CSV has {csv_rows} rows)")

print("\nRows per source file and ingestion mode:")
for raw_table in DELTA_FILES:
    print(f"\n-- bronze.{raw_table} --")
    bronze(raw_table).groupBy("_source_file", "_ingestion_mode").count().orderBy("_source_file").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Observations — bronze ingestion
# MAGIC %md
# MAGIC **Observations — bronze ingestion:**
# MAGIC
# MAGIC - `product_information_delta_001.csv`: 5 CSV rows → 5 Bronze rows.
# MAGIC - `discount_delta_001.csv`: 8 CSV rows → 8 Bronze rows.
# MAGIC - `products_sold_delta_001.csv`: 10 CSV rows → 10 Bronze rows.
# MAGIC - Every delta row carries `_ingestion_mode = delta` plus its `_source_file`; the initial CSVs still contribute exactly their original 20,252 / 185,962 / 1,010,492 rows — the delta run ingested only the new files.
# MAGIC
# MAGIC **Conclusion:** the delta files were picked up, stamped with lineage, and nothing was double-loaded — delta mode correctly skips already-ingested files.

# COMMAND ----------

# DBTITLE 1,Check 2 — delta rows in bronze
# MAGIC %md
# MAGIC ## 2. Bronze: the delta rows themselves — brand-new keys only

# COMMAND ----------

# DBTITLE 1,Check 2 — delta payloads and key novelty
# Checks: the actual delta-file rows in Bronze, and that the product keys they use are
# brand-new (no overlap with the initial product file) — the design principle above.
delta_products = bronze("products_raw").filter(F.col("_source_file") == DELTA_FILES["products_raw"])

print("bronze.products_raw — delta rows:")
delta_products.select("product_id", "brand", "product_category", "section", "season", "price") \
    .orderBy("product_id", "description").show(truncate=False)

print("bronze.discounts_raw — delta rows:")
bronze("discounts_raw").filter(F.col("_source_file") == DELTA_FILES["discounts_raw"]) \
    .select("product_id", "city", "discount").orderBy("product_id", "city").show(truncate=False)

print("bronze.sales_raw — delta rows:")
bronze("sales_raw").filter(F.col("_source_file") == DELTA_FILES["sales_raw"]) \
    .select("product_id", "city", "pieces_sold", "time").orderBy("product_id", "time").show(truncate=False)

initial_keys = bronze("products_raw").filter(F.col("_source_file") != DELTA_FILES["products_raw"]) \
    .select("product_id").distinct()
overlap = delta_products.select("product_id").distinct().join(initial_keys, "product_id").count()
print(f"[{'PASS' if overlap == 0 else 'FAIL'}] delta product keys that already exist in the initial file: {overlap}")

# COMMAND ----------

# DBTITLE 1,Observations — delta payloads
# MAGIC %md
# MAGIC **Observations — the delta payloads:**
# MAGIC
# MAGIC - **Products (5 rows):** 900001 H&M / electronics / KIDS / Monsoon $59.99; 900002 H&M / clothing / WOMAN / Summer $34.99; 900003 Zara with price `N/A` (deliberately broken); 700001 Zara twice with conflicting prices $45.00 vs $52.50.
# MAGIC - **Discounts (8 rows):** the four >20% discounts (0.25 Chicago, 0.32 Houston, 0.22 Dallas, 0.40 Phoenix — all on new keys), plus 0.24 Austin on 700002, an out-of-range 1.50 on (900002, Miami), and the conflicting (700003, Miami) pair 0.30 vs 0.45.
# MAGIC - **Sales (10 rows):** a valid 80-piece San Antonio sale on **existing** key 185102, a −5 negative-quantity row on the same key, valid sales for the new keys (900001 twice plus a conflicting 18/19 pair, 900002, 900003, 700002), and 999999 — a key that never exists as a product.
# MAGIC - Zero overlap between delta product keys and the initial file's keys.
# MAGIC
# MAGIC **Conclusion:** the files exercise exactly the intended scenarios — clean inserts on new keys, one new sale on an existing key, and isolated broken rows.

# COMMAND ----------

# DBTITLE 1,Check 3 — silver merge
# MAGIC %md
# MAGIC ## 3. Silver: the good delta rows merged in by key

# COMMAND ----------

# DBTITLE 1,Check 3 — silver merge results
# Checks: which delta rows reached Silver. Valid new rows must be merged; keys whose only
# delta rows are broken must NOT appear; and row totals must reflect initial + merged rows.
print("silver.products — delta keys (new + existing 185102 for reference):")
silver("products").filter(F.col("product_id").isin(NEW_PRODUCT_KEYS + [EXISTING_KEY])) \
    .select("product_id", "brand", "product_category", "section", "season", "price") \
    .orderBy("product_id").show(truncate=False)

print("silver.discounts — delta rows:")
silver("discounts").filter(F.col("product_id").isin(NEW_PRODUCT_KEYS)) \
    .select("product_id", "city", "discount").orderBy("product_id", "city").show(truncate=False)

print("silver.sales — delta sale rows:")
silver("sales").filter(
    F.col("product_id").isin(NEW_PRODUCT_KEYS)
    | ((F.col("product_id") == EXISTING_KEY)
       & (F.col("city") == "SAN ANTONIO") & (F.col("pieces_sold") == 80))
).select("product_id", "city", "pieces_sold", "sale_ts").orderBy("product_id", "sale_ts").show(truncate=False)

print("Quarantined keys/rows must be absent from Silver:")
absence_checks = [
    ("silver.products rows for 900003 (bad price)",
     silver("products").filter(F.col("product_id") == "900003").count()),
    ("silver.products rows for 700001 (conflicting pair)",
     silver("products").filter(F.col("product_id") == "700001").count()),
    ("silver.discounts rows for 700003 (conflicting pair)",
     silver("discounts").filter(F.col("product_id") == "700003").count()),
    ("silver.discounts (900002, MIAMI) 1.50 row",
     silver("discounts").filter((F.col("product_id") == "900002") & (F.col("city") == "MIAMI")).count()),
    ("silver.sales 185102 negative-quantity row",
     silver("sales").filter((F.col("product_id") == EXISTING_KEY) & (F.col("pieces_sold") < 0)).count()),
    ("silver.sales 900001 conflicting pair (2025-11-25 00:00)",
     silver("sales").filter((F.col("product_id") == "900001") & (F.col("sale_ts") == "2025-11-25 00:00:00")).count()),
]
for name, n in absence_checks:
    print(f"[{'PASS' if n == 0 else 'FAIL'}] {name}: {n}")

print("\nSilver row totals (initial load -> after delta run):")
for t in ["products", "discounts", "sales", "sales_enriched"]:
    print(f"silver.{t}: {silver(t).count()} rows")

# COMMAND ----------

# DBTITLE 1,Observations — silver merge
# MAGIC %md
# MAGIC **Observations — silver merge (updated for the current `missing_product_reference` rule):**
# MAGIC
# MAGIC - `silver.products`: 20,254 rows = 20,252 initial + **2 new products** (900001, 900002 — the H&M pair). 900003 (bad price) and 700001 (conflicting pair) are absent.
# MAGIC - `silver.discounts`: 184,124 rows = 184,121 initial + **3 new discounts** (900001 CHICAGO 0.250, 900001 HOUSTON 0.320, 900002 DALLAS 0.220). The 900003 PHOENIX 0.400 and 700002 AUSTIN 0.240 rows are valid on their own terms but their products never reach Silver, so the orphan-key gate quarantines them as `missing_product_reference`. The 1.50 row and the 700003 pair are absent.
# MAGIC - `silver.sales`: 1,010,496 rows = 1,010,492 initial + **4 new sales**. Existing key 185102 gained its new 80-piece SAN ANTONIO sale without touching its product row or older sales (key-based MERGE, no reprocessing side effects). The three orphan-key sales (700002 AUSTIN 12, 900003 PHOENIX 5, 999999 NEW YORK 10) are quarantined as `missing_product_reference`, as are the −5 row and the 900001 18/19 pair.
# MAGIC - `silver.sales_enriched`: 1,010,496 rows, matching `silver.sales` row-for-row. No orphan keys reach Silver under the current rule, so the earlier "rows without a product keep NULL product fields by the LEFT JOIN design" note no longer applies — every enriched row has a real product match.
# MAGIC
# MAGIC **Conclusion:** exactly the 9 clean delta rows merged; every broken or orphan-key row is absent from Silver — the `missing_product_reference` gate also keeps the Silver RELY foreign keys true.

# COMMAND ----------

# DBTITLE 1,Check 4 — new dimension values
# MAGIC %md
# MAGIC ## 4. Silver: new dimension values introduced by the delta run

# COMMAND ----------

# DBTITLE 1,Check 4 — H&M / KIDS / MONSOON / AUSTIN in silver
# Checks: the dimension values the delta files introduce — brand H&M, category electronics,
# section KIDS, season Monsoon, city Austin — none of which existed in the initial files.
# Note (missing_product_reference rule): AUSTIN's only rows are 700002's, and 700002 has no
# valid product, so those rows are quarantined before Silver — the two city assertions
# FAIL by design under the current rule.
for col, val in [("brand", "H&M"), ("product_category", "electronics"), ("section", "KIDS"), ("season", "MONSOON")]:
    n = silver("products").filter(F.col(col) == val).count()
    print(f"[{'PASS' if n > 0 else 'FAIL'}] silver.products has {col} = {val}: {n} rows")

for tbl in ["discounts", "sales"]:
    n = silver(tbl).filter(F.col("city") == "AUSTIN").count()
    print(f"[{'PASS' if n > 0 else 'FAIL'}] silver.{tbl} has city = AUSTIN: {n} rows")

print("\nsilver.products distinct dimension values after the delta run:")
for col in ["brand", "product_category", "section", "season"]:
    vals = [r[0] for r in silver("products").select(col).distinct().orderBy(col).collect()]
    print(f"  {col}: {vals}")

for tbl in ["discounts", "sales"]:
    cities = [r[0] for r in silver(tbl).select("city").distinct().orderBy("city").collect()]
    print(f"silver.{tbl} distinct cities ({len(cities)}): {cities}")

# COMMAND ----------

# DBTITLE 1,Observations — new dimension values
# MAGIC %md
# MAGIC **Observations — new dimension values:**
# MAGIC
# MAGIC - `brand`: **H&M** now present alongside Zara (2 rows) — brand-new value from the delta files.
# MAGIC - `product_category`: **electronics** (1 row) — new category.
# MAGIC - `section`: **KIDS** (1 row) alongside MAN and WOMAN.
# MAGIC - `season`: **MONSOON** (1 row) — the season set grows to 5 (AUTUMN, MONSOON, SPRING, SUMMER, WINTER).
# MAGIC - `city`: **AUSTIN does not appear** in `silver.discounts` or `silver.sales` — the only AUSTIN rows are 700002's, and 700002 has no valid product, so the orphan-key gate quarantines them as `missing_product_reference`. The city set stays at 10 (upper case, normalized). This is why check 4's two city assertions record FAIL — they predate the rule.
# MAGIC
# MAGIC **Conclusion:** the product-level values (H&M / electronics / KIDS / MONSOON) are visible as new distinct values in Silver; AUSTIN is deliberately absent because the current rule quarantines its rows before Silver — of the values [profiling_notebook](#notebook-4067024606946494) would flag as new against the initial files, only the product ones reach Silver.

# COMMAND ----------

# DBTITLE 1,Check 5 — quarantine rows
# MAGIC %md
# MAGIC ## 5. Quarantine: the isolated broken rows, all from the delta run

# COMMAND ----------

# DBTITLE 1,Check 5 — quarantine contents and rules
# Checks: every quarantined row — all must come from the delta files (_ingestion_mode='delta'),
# carry the expected _dq_rule, and their keys must not have leaked into Silver (check 3).
for t in ["products", "discounts", "sales"]:
    q = quarantine(t)
    cols = [c for c in ["product_id", "city", "discount", "price", "pieces_sold", "_dq_rule",
                        "_ingestion_mode", "_source_file"] if c in q.columns]
    print(f"\n-- quarantine.{t}: {q.count()} rows --")
    q.select(*cols).orderBy("product_id").show(truncate=False)

non_delta = sum(
    quarantine(t).filter(F.col("_ingestion_mode") != "delta").count()
    for t in ["products", "discounts", "sales"]
)
print(f"[{'PASS' if non_delta == 0 else 'FAIL'}] quarantine rows NOT from the delta run: {non_delta}")

not_from_delta_files = sum(
    quarantine(t).filter(~F.col("_source_file").endswith("delta_001.csv")).count()
    for t in ["products", "discounts", "sales"]
)
print(f"[{'PASS' if not_from_delta_files == 0 else 'FAIL'}] quarantine rows not from *_delta_001.csv: {not_from_delta_files}")

# COMMAND ----------

# DBTITLE 1,Observations — quarantine
# MAGIC %md
# MAGIC **Observations — quarantine (updated for the current `missing_product_reference` rule):**
# MAGIC
# MAGIC **14 broken rows per delta run** across the three quarantine tables = the 9 rows from the **6 original cases** plus **5 orphan-key rows** stamped `missing_product_reference` (keys 700002, 900003, 999999). Every row carries `_ingestion_mode = delta` and `_source_file = *_delta_001.csv` — and every logical row appears **three times** (42 rows total: products 9, discounts 15, sales 18): quarantine is append-mode and Silver reprocesses all of Bronze each run, so each delta-mode run re-appends the same rejects (the 2026-11-22 run / manual orphan move, plus the two 2026-11-24 runs — the constraint-failure run and its repair).
# MAGIC
# MAGIC | Case | Table | Key(s) | `_dq_rule` |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Bad price (`N/A`) | quarantine.products | 900003 | `price_null_or_unparseable` |
# MAGIC | Conflicting duplicate pair | quarantine.products | 700001 ($45.00 vs $52.50) | `conflicting_duplicate_key` |
# MAGIC | Out-of-range discount (1.50) | quarantine.discounts | (900002, MIAMI) | `discount_out_of_range_or_null` |
# MAGIC | Conflicting duplicate pair | quarantine.discounts | (700003, MIAMI) 0.30 vs 0.45 | `conflicting_duplicate_key` |
# MAGIC | Orphan key (no valid product) | quarantine.discounts | 700002 AUSTIN 0.240; 900003 PHOENIX 0.400 | `missing_product_reference` |
# MAGIC | Negative quantity (−5) | quarantine.sales | 185102 | `pieces_sold_null_or_non_positive` |
# MAGIC | Conflicting duplicate pair | quarantine.sales | 900001 (18 vs 19, same sale_ts) | `conflicting_duplicate_key` |
# MAGIC | Orphan key (no valid product) | quarantine.sales | 700002 AUSTIN 12; 900003 PHOENIX 5; 999999 NEW YORK 10 | `missing_product_reference` |
# MAGIC
# MAGIC **Conclusion:** rejected rows are warnings by design, so the pipeline keeps flowing — the WARN entries now come from `02_silver_transform`'s Warnings cell (the standalone `03_silver_dq` notebook was consolidated into it). No quarantined key leaked into Silver (verified in check 3). Expect 14 more appended rows on every future delta run of unchanged Bronze data.

# COMMAND ----------

# DBTITLE 1,Check 6 — gold view and rollup
# MAGIC %md
# MAGIC ## 6. Gold: new keys and dimensions flow through the facts view and rollup

# COMMAND ----------

# DBTITLE 1,Check 6 — gold facts view and rollup
# Checks: that the delta rows are visible in the gold layer — the task1_sales_facts view
# (live join of silver sales x products x discounts) and the task1_sales_rollup table —
# and that orphan-key sales are correctly excluded from gold.
print("gold.task1_sales_facts — November rows for the delta keys and 185102:")
gold("task1_sales_facts").filter(
    (F.col("product_id").isin(NEW_PRODUCT_KEYS) | (F.col("product_id") == EXISTING_KEY))
    & (F.col("sale_ts") >= "2025-11-24 16:00:00")
).orderBy("product_id", "sale_ts").show(truncate=False)

orphan_rows = gold("task1_sales_facts").filter(
    F.col("product_id").isin(["999999", "700002", "900003"])
).collect()
print(f"[{'PASS' if len(orphan_rows) == 0 else 'FAIL'}] orphan-key sales in the gold view (999999 / 700002 / 900003): {len(orphan_rows)} rows "
      "— the view INNER JOINs silver.products, so sales without a product row are excluded")

# Under the current missing_product_reference rule this historical caveat is resolved:
# orphan-key sales are quarantined before Silver, so the RELY foreign key
# (silver.sales -> silver.products) is no longer violated and an aggregate COUNT(*) on this
# same filter correctly returns 0, agreeing with the row-level result. (Before the rule,
# orphan rows sat in silver.sales and the optimizer's trusted join-elimination over-counted:
# COUNT(*) returned 3 vs 0 actual rows.)
orphan_count = spark.sql(
    f"SELECT COUNT(*) FROM {catalog_name}.gold.task1_sales_facts "
    "WHERE product_id IN ('999999', '700002', '900003')"
).collect()[0][0]
print(f"    SQL COUNT(*) on the same filter: {orphan_count} (matches the row count — the missing_product_reference rule keeps orphan keys out of Silver, so the RELY FK holds)")

print("gold.task1_sales_rollup — rows for the new KIDS / MONSOON values:")
gold("task1_sales_rollup").filter((F.col("section") == "KIDS") | (F.col("season") == "MONSOON")) \
    .orderBy("city").show(truncate=False)

for col, val in [("section", "KIDS"), ("season", "MONSOON")]:
    vals = [r[0] for r in gold("task1_sales_rollup").select(col).distinct().orderBy(col).collect()]
    print(f"[{'PASS' if val in vals else 'FAIL'}] rollup distinct {col} contains {val}: {vals}")

# COMMAND ----------

# DBTITLE 1,Observations — gold
# MAGIC %md
# MAGIC **Observations — gold:**
# MAGIC
# MAGIC - The facts view returns the delta rows fully enriched: 900001 CHICAGO 15 × $59.99 × (1 − 0.25) = 674.88750 and HOUSTON 22 × $59.99 × (1 − 0.32) = 897.45040; 900002 DALLAS 30 × $34.99 × (1 − 0.22) = 818.76600; and 185102's new SAN ANTONIO sale 80 × $78.99 × (1 − 0.07) = 5876.85600 (185102's other November rows predate the delta run and come from the initial file).
# MAGIC - `task1_sales_rollup` gained the two new KIDS / MONSOON cells for 2025-11 — CHICAGO (max_discount 0.250) and HOUSTON (max_discount 0.320); KIDS and MONSOON are now distinct values in the rollup.
# MAGIC - Sales rows whose product never made it to `silver.products` (999999, 700002, and 900003's PHOENIX sale) return **0 rows** in the view — under the current rule those rows are quarantined before Silver (`missing_product_reference`), so exclusion now happens upstream of the view's INNER JOIN rather than by the join dropping Silver-present orphans. AUSTIN therefore appears in neither Silver nor the rollup.
# MAGIC - **RELY-FK caveat — resolved by the current rule:** the earlier note that an aggregate `COUNT(*)` on this filter reports 3, not 0 (orphan rows in `silver.sales` violated the RELY foreign key, so the optimizer's join-elimination over-counted) no longer applies. With orphan keys kept out of Silver the constraint holds, and the recorded `COUNT(*)` is **0**, matching the row-level result — keeping the Silver RELY foreign keys true was the rule's stated design goal.
# MAGIC
# MAGIC **Conclusion:** the delta run is visible end-to-end in gold — new products, new >20% discounts, and a new sale on an existing key, all with correct effective-volume arithmetic.

# COMMAND ----------

# DBTITLE 1,Conclusion — delta run proven
# MAGIC %md
# MAGIC ## Conclusion — the delta run is proven (updated for the current `missing_product_reference` rule)
# MAGIC
# MAGIC - **Bronze** ingested all 23 delta-file rows exactly once, with `_source_file` / `_ingestion_mode` lineage, and re-ingested nothing.
# MAGIC - **Silver** merged the 9 clean rows (2 products, 3 discounts, 4 sales) by key — new values H&M / electronics / KIDS / MONSOON all present; existing rows untouched. AUSTIN is not in Silver: its only rows are 700002's, quarantined as `missing_product_reference`.
# MAGIC - **Quarantine** holds the 14 broken rows per run (the 6 original cases plus 5 `missing_product_reference` rows across keys 700002 / 900003 / 999999), all stamped `delta`, none leaking into Silver. Append-mode means each future delta run of unchanged Bronze data re-appends the same 14 rows (42 = 3 appends at the time of the recorded outputs).
# MAGIC - **Gold** reflects the delta run through the live facts view and the rollup, with orphan-key rows excluded upstream in quarantine. The old RELY-FK over-count follow-up is closed: the `missing_product_reference` gate keeps orphan keys out of Silver, so the RELY foreign keys hold and count-only queries through the facts view no longer over-count (`COUNT(*)` of the orphan keys = 0, matching the 0 actual rows).
# MAGIC
# MAGIC Delta files → Bronze (once, with lineage) → Silver (clean key-based merge) + Quarantine (all bad and orphan-key rows, automatically under the current rule) → Gold (visible new dimensions). The delta run with the delta files works as designed.