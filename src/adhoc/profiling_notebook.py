# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Data Profiling Notebook (pre pipeline exploratory checks)
# MAGIC
# MAGIC Profiles `product_information.csv`, `discount.csv`, `products_sold.csv` as landed in
# MAGIC the raw-files Volume. Each cell below checks one specific data-quality concern and is followed by an Observations / Conclusion markdown cell summarizing what the check found

# COMMAND ----------

# DBTITLE 1,Quick read — profiling summary
# MAGIC %md
# MAGIC ## Quick read — profiling summary
# MAGIC
# MAGIC **Bottom line: the three files are clean and load-ready. The only required fix is deduplicating `discount.csv` on `(Product ID, city)`** — 1,841 duplicated keys (3,682 rows), all exact copies, so the dedupe is lossless: 185,962 → 184,121 rows.
# MAGIC
# MAGIC | File | Rows | Verified key | Notable |
# MAGIC | --- | --- | --- | --- |
# MAGIC | product_information.csv | 20,252 | `Product ID` unique | nulls only in `name` (1), `description` (2); price $12.00–$134.99 |
# MAGIC | discount.csv | 185,962 | `(Product ID, city)` unique **after dedup** | fraction scale 0–0.2 — treat as fraction, never divide by 100 |
# MAGIC | products_sold.csv | 1,010,492 | `(Product ID, city, time)` unique | `pieces_sold` 1–99; no nulls, negatives, or zeros |
# MAGIC
# MAGIC **Cross-file integrity — all clean:** 0 orphan Product IDs; 0 sold `(Product ID, city)` pairs without a discount row; identical 10-city sets with no spelling variants; all categorical values already normalized (no case/whitespace variants). `brand` (Zara), `Product Category` (clothing), `currency` (USD) are constants — useless as analysis dimensions.
# MAGIC
# MAGIC **Analysis caveats:**
# MAGIC - `time` is Unix epoch seconds: 2025-01-01 → 2025-11-29 — **no December data**; any "yearly" analysis is really Jan–Nov.
# MAGIC - Every product sold at least once; best/worst are **exact ties** (202 products @ 9,801 pieces; 201 @ 1 piece) — compare best/worst as groups, not single products.
# MAGIC - The quantile bucketing of products sold provides roughly equal-sized best/worst cohorts suitable for attribute-level comparison.
# MAGIC - Quantities skew high (median 70 of a 1–99 range) — a synthetic-data signature to remember when interpreting volume metrics.
# MAGIC
# MAGIC Details: each check below is followed by its **Observations / Conclusion** cell.

# COMMAND ----------

# DBTITLE 1,Widget defaults
dbutils.widgets.text("catalog", "sales_data_cc", "Catalog name")
dbutils.widgets.text("schema_name", "landing", "Schema name")
dbutils.widgets.text("volume_name", "raw_files", "Volume name (for landed CSVs)")

catalog_name = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
VOLUME_BASE = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

# COMMAND ----------

import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 160)

# Volumes are FUSE-mounted, so pandas can read the CSVs directly by path.
PRODUCT_INFO_PATH = f"{VOLUME_BASE}/product_information.csv"
DISCOUNT_PATH = f"{VOLUME_BASE}/discount.csv"
PRODUCTS_SOLD_PATH = f"{VOLUME_BASE}/products_sold.csv"

product_info = pd.read_csv(PRODUCT_INFO_PATH)
discount = pd.read_csv(DISCOUNT_PATH)
products_sold = pd.read_csv(PRODUCTS_SOLD_PATH)

# Normalize column access (strip whitespace from header names only).
product_info.columns = [c.strip() for c in product_info.columns]
discount.columns = [c.strip() for c in discount.columns]
products_sold.columns = [c.strip() for c in products_sold.columns]

PID_INFO = "Product ID"
PID_DISC = "Product ID"
PID_SOLD = "Product ID"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Columns, types, row counts, null counts per file

# COMMAND ----------

# Checks: row count, column dtypes, and null counts for each of the three files.
def file_summary(name, df):
    print(f"\n=== {name} ===")
    print(f"rows: {len(df)}")
    print("dtypes:")
    print(df.dtypes)
    print("null counts:")
    print(df.isna().sum())


for name, df in [
    ("product_information.csv", product_info),
    ("discount.csv", discount),
    ("products_sold.csv", products_sold),
]:
    file_summary(name, df)

# COMMAND ----------

# DBTITLE 1,Observations — file summary
# MAGIC %md
# MAGIC **Observations — file summary:**
# MAGIC
# MAGIC - `product_information.csv`: 20,252 rows × 16 columns; only nulls are `name` (1) and `description` (2).
# MAGIC - `discount.csv`: 185,962 rows × 3 columns; no nulls.
# MAGIC - `products_sold.csv`: 1,010,492 rows × 4 columns; no nulls.
# MAGIC - Types load sanely as-is: `Product ID` int64 in all three files, `price`/`discount` float64, `pieces_sold`/`time` int64.
# MAGIC
# MAGIC **Conclusion:** the files are essentially complete (3 null cells out of ~1.2M rows) — no null-handling needed beyond leaving the two `name`/`description` blanks as-is.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. product_information: price column, distinct categorical values

# COMMAND ----------

# Checks: whether a 'price' column exists and its distribution (min/max/mean/quartiles).
print("Has 'price' column:", "price" in product_info.columns)
if "price" in product_info.columns:
    print(product_info["price"].describe())

# COMMAND ----------

# DBTITLE 1,Observations — price
# MAGIC %md
# MAGIC **Observations — price:** present in all 20,252 rows; $12.00–$134.99, mean $41.95, median $35.95 (IQR $23.95–$53.95).
# MAGIC
# MAGIC **Conclusion:** plausible USD retail range with no zero, negative, or extreme values — `price` is clean and usable as-is.

# COMMAND ----------

# Checks: distinct values of a categorical column, plus case/whitespace variants that
# would otherwise be treated as different categories (e.g. "Winter" vs. " winter").
def distinct_value_report(df, col):
    """Show raw distinct values plus case/whitespace-normalized groupings."""
    if col not in df.columns:
        print(f"Column '{col}' not found.")
        return
    raw_values = df[col].dropna().unique()
    print(f"\n--- '{col}': {len(raw_values)} raw distinct value(s) ---")
    print(sorted(raw_values.tolist(), key=str))

    normalized = df[col].dropna().astype(str).str.strip().str.lower()
    norm_counts = normalized.value_counts()
    print(f"'{col}' normalized (trim+lower) distinct count: {norm_counts.shape[0]}")

    # Flag normalized groups that map back to more than one raw variant.
    variant_map = df[[col]].dropna().copy()
    variant_map["__norm"] = normalized
    variants = (
        variant_map.groupby("__norm")[col]
        .apply(lambda s: sorted(set(s)))
        .loc[lambda s: s.apply(len) > 1]
    )
    if len(variants):
        print(f"Case/whitespace variants detected in '{col}':")
        print(variants)
    else:
        print(f"No case/whitespace variants detected in '{col}'.")


for col in ["section", "season", "Product Category", "Seasonal", "Product Position", "Promotion", "brand", "currency", "terms", "material", "origin"]:
    distinct_value_report(product_info, col)

# COMMAND ----------

# DBTITLE 1,Observations — categorical values
# MAGIC %md
# MAGIC **Observations — categoricals:** every categorical column has a small, expected value set with zero case/whitespace variants: `section` 2, `season` 4, `Product Position` 3, `Seasonal`/`Promotion` 2, `terms` 5, `material` 11, `origin` 12. `brand` (Zara), `Product Category` (clothing), and `currency` (USD) are single-valued constants.
# MAGIC
# MAGIC **Conclusion:** no normalization needed before load. The three constant columns can't differentiate products, so they're ignorable as analysis dimensions (kept for completeness).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Is `Product ID` unique in product_information?

# COMMAND ----------

# Checks: `Product ID` is a valid primary key (no duplicate rows) in product_information.
dup_info_ids = product_info[product_info.duplicated(PID_INFO, keep=False)]
print("product_information Product ID unique:", product_info[PID_INFO].is_unique)
print("duplicate rows (if any):")
print(dup_info_ids.sort_values(PID_INFO))

# COMMAND ----------

# DBTITLE 1,Observations — Product ID uniqueness
# MAGIC %md
# MAGIC **Observations:** `Product ID` is unique across all 20,252 rows — no duplicate rows.
# MAGIC
# MAGIC **Conclusion:** `Product ID` is a valid primary key for `product_information`; joins from `discount` and `products_sold` are safe on this key.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. discount: (Product ID, city) uniqueness, scale, out-of-range values

# COMMAND ----------

# Checks: (Product ID, city) is a valid composite key (no duplicates) in discount.
dup_discount_keys = discount[discount.duplicated([PID_DISC, "city"], keep=False)]
print("discount (Product ID, city) unique:", not discount.duplicated([PID_DISC, "city"]).any())
print(f"duplicate (Product ID, city) rows: {len(dup_discount_keys)}")
print(dup_discount_keys.sort_values([PID_DISC, "city"]).head(20))

# Additional check: for duplicated keys, is the discount value identical or conflicting?
if len(dup_discount_keys):
    dup_discount_summary = (
        dup_discount_keys.groupby([PID_DISC, "city"])["discount"]
        .agg(["nunique", "min", "max", "size"])
        .reset_index()
    )
    conflicting_keys = dup_discount_summary[dup_discount_summary["nunique"] > 1]
    print(f"\nOf {len(dup_discount_summary)} duplicated keys, {len(conflicting_keys)} have differing discount values:")
    print(conflicting_keys.head(20))

# COMMAND ----------

# DBTITLE 1,Observations — discount key duplicates
# MAGIC %md
# MAGIC **Observations:** `(Product ID, city)` is **not** unique in `discount`: 1,841 keys are duplicated (3,682 rows involved) — but 0 of them have conflicting `discount` values; every duplicate is an exact copy.
# MAGIC
# MAGIC **Conclusion:** drop exact duplicates on `(Product ID, city)` before load (185,962 → 184,121 rows). No ambiguity about which copy to keep, since all copies agree.

# COMMAND ----------

# Checks: the scale of the `discount` column (fraction vs. percentage) and whether any
# values fall outside the plausible [0,1] or [0,100] ranges.
print("discount column stats:")
print(discount["discount"].describe())
print("min:", discount["discount"].min(), "max:", discount["discount"].max())

scale_guess = "0-1" if discount["discount"].max() <= 1 else "0-100 (or mixed)"
print("Likely scale:", scale_guess)

out_of_range_0_1 = discount[(discount["discount"] < 0) | (discount["discount"] > 1)]
print(f"Values outside [0,1]: {len(out_of_range_0_1)}")
print(out_of_range_0_1.head(20))

out_of_range_0_100 = discount[(discount["discount"] < 0) | (discount["discount"] > 100)]
print(f"Values outside [0,100]: {len(out_of_range_0_100)}")
print(out_of_range_0_100.head(20))

# COMMAND ----------

# DBTITLE 1,Observations — discount scale
# MAGIC %md
# MAGIC **Observations:** `discount` is on a 0–1 fraction scale: min 0.0, max 0.2, mean 0.10, quartiles at 0.05/0.10/0.15 — 0 values outside [0,1].
# MAGIC
# MAGIC **Conclusion:** treat `discount` as a fraction (do not divide by 100); a [0,1] validation in silver is safe and will pass.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. products_sold: (Product ID, city, time) uniqueness, pieces checks, date range

# COMMAND ----------

# Checks: (Product ID, city, time) is a valid composite key (no duplicates) in products_sold.
dup_sold_keys = products_sold[products_sold.duplicated([PID_SOLD, "city", "time"], keep=False)]
print(
    "products_sold (Product ID, city, time) unique:",
    not products_sold.duplicated([PID_SOLD, "city", "time"]).any(),
)
print(f"duplicate (Product ID, city, time) rows: {len(dup_sold_keys)}")
print(dup_sold_keys.sort_values([PID_SOLD, "city", "time"]).head(20))

# COMMAND ----------

# DBTITLE 1,Observations — sold key uniqueness
# MAGIC %md
# MAGIC **Observations:** `(Product ID, city, time)` is unique across all 1,010,492 rows — 0 duplicates.
# MAGIC
# MAGIC **Conclusion:** `(Product ID, city, time)` is the valid grain of the sales fact; no dedup needed for `products_sold`.

# COMMAND ----------

# Checks: `pieces_sold` for nulls, negative values, zeros, and its overall distribution.
pieces = products_sold["pieces_sold"]
print(f"pieces_sold null count:     {pieces.isna().sum():,}")
print(f"pieces_sold negative count: {(pieces < 0).sum():,}")
print(f"pieces_sold zero count:     {(pieces == 0).sum():,}")

stats = pieces.describe()
print("\npieces_sold distribution:")
print(f"  count:  {int(stats['count']):,}")
print(f"  mean:   {stats['mean']:.2f}")
print(f"  std:    {stats['std']:.2f}")
print(f"  min:    {int(stats['min']):,}")
print(f"  25%:    {int(stats['25%']):,}")
print(f"  50%:    {int(stats['50%']):,}")
print(f"  75%:    {int(stats['75%']):,}")
print(f"  max:    {int(stats['max']):,}")

# COMMAND ----------

# DBTITLE 1,Observations — pieces_sold
# MAGIC
# MAGIC %md
# MAGIC **Observations:** `pieces_sold` has 0 nulls, 0 negatives, 0 zeros. Range 1–99, mean 66.2, median 70 — quantities skew high (median 70 out of a 1–99 range), unlike typical real retail where low quantities dominate.
# MAGIC
# MAGIC **Conclusion:** no cleaning needed. The high-quantity skew is worth remembering when interpreting volume metrics (likely a synthetic-data signature).

# COMMAND ----------

# Checks: `time` parses as a Unix epoch (seconds) and reports the resulting date range.
# `time` appears to be a Unix epoch (seconds) integer; confirm format and range.
time_col = products_sold["time"]
print("time dtype:", time_col.dtype)
print("time null count:", time_col.isna().sum())

time_as_dt = pd.to_datetime(time_col, unit="s", utc=True, errors="coerce")
print("Unparsable as epoch seconds:", time_as_dt.isna().sum() - time_col.isna().sum())
print("time (epoch) min/max:", time_col.min(), time_col.max())
print("time (as datetime) min/max:", time_as_dt.min(), time_as_dt.max())

# COMMAND ----------

# DBTITLE 1,Observations — time epoch
# MAGIC %md
# MAGIC **Observations:** `time` is int64 Unix epoch seconds; 0 nulls and 0 unparseable values. Range: 2025-01-01 00:00:04 → 2025-11-29 23:59:10.
# MAGIC
# MAGIC **Conclusion:** cast from epoch seconds to a timestamp in silver. Coverage is ~11 months of 2025 with no December data — any "yearly" analysis is really Jan–Nov.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Orphans and city spelling variants

# COMMAND ----------

# Checks: `Product ID`s in products_sold that have no matching row in product_information.
info_ids = set(product_info[PID_INFO].dropna().unique())
sold_ids = set(products_sold[PID_SOLD].dropna().unique())
discount_ids = set(discount[PID_DISC].dropna().unique())

print(f"discount Product IDs missing from product_information: {len(discount_ids - info_ids)}")
orphan_discount_ids = discount_ids - info_ids
print(sorted(orphan_discount_ids))

orphan_sold_ids = sold_ids - info_ids
print(f"products_sold Product IDs missing from product_information: {len(orphan_sold_ids)}")
print(sorted(orphan_sold_ids))

# COMMAND ----------

# DBTITLE 1,Observations — orphan product IDs
# MAGIC %md
# MAGIC **Observations:** 0 `discount` `Product ID`s and 0 `products_sold` `Product ID`s are missing from `product_information` (orphan lists are empty).
# MAGIC
# MAGIC **Conclusion:** referential integrity holds on the product dimension for both tables — sales and discounts, can be enriched with product attributes without dropping or guessing rows.

# COMMAND ----------

# Checks: (Product ID, city) pairs sold that have no matching discount entry.
sold_pairs = set(map(tuple, products_sold[[PID_SOLD, "city"]].dropna().values.tolist()))
discount_pairs = set(map(tuple, discount[[PID_DISC, "city"]].dropna().values.tolist()))

orphan_sold_pairs = sold_pairs - discount_pairs
print(f"products_sold (Product ID, city) pairs with no matching discount row: {len(orphan_sold_pairs)}")
for pair in sorted(orphan_sold_pairs)[:20]:
    print(pair)

# COMMAND ----------

# DBTITLE 1,Observations — orphan sold pairs
# MAGIC %md
# MAGIC **Observations:** 0 sold `(Product ID, city)` pairs lack a discount row.
# MAGIC
# MAGIC **Conclusion:** every sale joins to a discount — missing-discount handling (e.g. defaulting to 0) is not needed in practice, though keeping it as a safety net is harmless.

# COMMAND ----------

# Checks: city names for case/whitespace variants within a file, and whether the
# normalized set of cities matches between discount and products_sold.
def city_variant_report(label, df):
    raw_cities = df["city"].dropna().astype(str)
    normalized = raw_cities.str.strip().str.lower()
    variant_map = pd.DataFrame({"raw": raw_cities, "norm": normalized})
    variants = (
        variant_map.groupby("norm")["raw"]
        .apply(lambda s: sorted(set(s)))
        .loc[lambda s: s.apply(len) > 1]
    )
    print(f"\n--- city variants in {label} ---")
    if len(variants):
        print(variants)
    else:
        print("No case/whitespace city variants detected.")
    return normalized.unique()


norm_cities_discount = city_variant_report("discount", discount)
norm_cities_sold = city_variant_report("products_sold", products_sold)

cross_file_norm_discount = set(norm_cities_discount)
cross_file_norm_sold = set(norm_cities_sold)
print("\nNormalized city sets equal across discount/products_sold:", cross_file_norm_discount == cross_file_norm_sold)
print("In discount but not products_sold (normalized):", cross_file_norm_discount - cross_file_norm_sold)
print("In products_sold but not discount (normalized):", cross_file_norm_sold - cross_file_norm_discount)

# COMMAND ----------

# DBTITLE 1,Observations — city variants
# MAGIC %md
# MAGIC **Observations:** no case/whitespace city variants within either file, and the normalized city sets are identical across `discount` and `products_sold` (the same 10 US cities).
# MAGIC
# MAGIC **Conclusion:** `city` is join-safe as-is — no city normalization needed anywhere in the pipeline.

# COMMAND ----------

# Checks: distribution of total products sold per Product ID, including products with zero sales.
# A product with 0 sales = a Product ID in product_information that has no rows in products_sold.
pieces_per_product = (
    products_sold.groupby(PID_SOLD)["pieces_sold"]
    .sum()
    .rename("total_pieces_sold")
    .reset_index()
)

# Left join from product_information so unsold products appear with 0 sales.
sales_per_product = product_info[[PID_INFO]].merge(
    pieces_per_product, left_on=PID_INFO, right_on=PID_SOLD, how="left"
)
sales_per_product["total_pieces_sold"] = sales_per_product["total_pieces_sold"].fillna(0).astype(int)
sales_per_product = sales_per_product.drop(columns=[PID_SOLD])

print(f"Products in product_information: {len(sales_per_product)}")
print(f"Products with 0 sales: {(sales_per_product['total_pieces_sold'] == 0).sum()}")
print(f"Products with >0 sales: {(sales_per_product['total_pieces_sold'] > 0).sum()}")

max_pieces = sales_per_product["total_pieces_sold"].max()
min_pieces = sales_per_product["total_pieces_sold"].min()
print(f"Max pieces sold: {max_pieces} (achieved by {(sales_per_product['total_pieces_sold'] == max_pieces).sum()} distinct products)")
print(f"Min pieces sold: {min_pieces} (achieved by {(sales_per_product['total_pieces_sold'] == min_pieces).sum()} distinct products)")

print("\n--- Distribution of total pieces sold per product ---")
print(sales_per_product["total_pieces_sold"].describe())

print("\n--- Distribution buckets ---")
# Buckets are chosen for the goal: picking the best and worst products to compare.
# - "0" isolates products that never sold (the clearest "worst" candidates).
# - Products with sales are split at sales QUANTILES (not fixed widths), so the bottom
#   decile and top decile buckets hold ~equal numbers of products, making the worst-selling and best-selling groups directly comparable regardless of how skewed the sales distribution is (fixed-width bins like 1-10 / 101-500 would lump most products into one bucket and leave the extremes nearly empty).
zero_count = int((sales_per_product["total_pieces_sold"] == 0).sum())
nonzero = sales_per_product.loc[sales_per_product["total_pieces_sold"] > 0, "total_pieces_sold"]
quantile_buckets = (
    pd.qcut(nonzero, q=[0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0], duplicates="drop")
    .value_counts()
    .sort_index()
)

rows = [{"bucket": "0 (never sold)", "num_products": zero_count}]
for interval, count in quantile_buckets.items():
    lo = max(int(round(interval.left)), 1)
    hi = int(round(interval.right))
    label = f"{lo}-{hi}" if lo != hi else str(lo)
    rows.append({"bucket": label, "num_products": int(count)})
dist_table = pd.DataFrame(rows)
dist_table["pct_of_products"] = (dist_table["num_products"] / len(sales_per_product) * 100).round(2)
print(dist_table.to_string(index=False))

# Optional detail: the extremes, to pick specific best/worst products for comparison.
sold_products = sales_per_product[sales_per_product["total_pieces_sold"] > 0]
print("\n--- Top 10 best-selling products ---")
print(sold_products.sort_values("total_pieces_sold", ascending=False).head(10))
print("\n--- Bottom 10 worst-selling products (among products with sales) ---")
print(sold_products.sort_values("total_pieces_sold").head(10))


# COMMAND ----------

# DBTITLE 1,Observations — sales distribution per product
# MAGIC %md
# MAGIC **Observations:** all 20,252 products sold at least once (0 zero-sale products). Total pieces per product: 1 to 9,801, mean 3,305, median 2,500. The extremes are exact ties: the max of 9,801 pieces is shared by 202 products, and the min of 1 piece is shared by 201 products. Quantile buckets give comparable group sizes (2K products per extreme decile). 
# MAGIC
# MAGIC **Conclusion:** "best" and "worst" products cannot be single picks — they must be compared as groups. The quantile bucketing provides roughly equal-sized best/worst cohorts suitable for attribute-level comparison.