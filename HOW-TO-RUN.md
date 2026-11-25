# How to run this project

This repo *is* a Databricks Asset Bundle: two Jobs (`sales_data_cc_seed`,
`sales_data_cc_pipeline`) and two Lakeview dashboards. Everything below is done in the
Databricks workspace UI — no CLI needed. For what the pipeline does and why, see
**`README.md`**.

## 1. Deploy

1. Open this Git folder in the Databricks workspace.
2. Open the 🚀 **Deployments** panel → deploy the `dev` target.

This creates/updates both Jobs, both dashboards, and uploads `data/` (the source CSVs).

## 2. Run — baseline load

1. **Workflows → `sales_data_cc_seed` → Run now** (default parameters). Seeds
   `data/initial/*.csv` into the landing Volume.
2. **Workflows → `sales_data_cc_pipeline` → Run now.** It also has a file-arrival trigger
   on that Volume, but the Volume doesn't exist until step 1 creates it, so don't rely on
   the trigger for this first run — run it manually (always safe, every stage rebuilds in
   full).
3. Open either dashboard under **Dashboards**.

## 3. Run — delta load (optional, tests incremental processing)

1. **Workflows → `sales_data_cc_seed` → Run now**, setting parameter
   `seed_delta_files = true`. Copies the small `data/delta/*.csv` batch in addition.
2. **Workflows → `sales_data_cc_pipeline` → Wait a minute or two —
   the trigger should fire on its own. If not press Run now** again
3. Open `src/adhoc/delta_processing_check.py` in the workspace and run it manually — it
   confirms the new rows merged into Silver and the bad rows landed in `quarantine.*`
   (expected quarantine reasons: `price_null_or_unparseable`, `conflicting_duplicate_key` ×3,
   `discount_out_of_range_or_null`, `pieces_sold_null_or_non_positive`,
   `missing_product_reference` ×2 — see `README.md` §7 for details).
4. You can also eyeball it in the **"Gold Task 1 — Sales by City"** dashboard: its
   **Product section** / **Product season** filter widgets now offer `KIDS` / `Monsoon` as
   selectable values — new dimensions introduced only by the delta batch's product
   `900001`, which isn't one of the quarantined rows.

`src/adhoc/profiling_notebook.py` is another standalone notebook (not part of either Job) —
run it manually for raw-CSV profiling at any point. It always profiles only the original
`product_information.csv` / `discount.csv` / `products_sold.csv`
