# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Seed landing Volume (simulated file drop)
# MAGIC Simulates an external actor dropping CSVs into the landing Volume — standing in for
# MAGIC whatever real upstream system would normally land files there. This is the only
# MAGIC task of the `sales_data_cc_seed` Job, separate from `sales_data_cc_pipeline` (the
# MAGIC medallion `bronze → silver → gold` Job) and not `%run`-included by anything else; it
# MAGIC uses its own small, minimal widget/config set (below), not `common/config.py`.
# MAGIC
# MAGIC This Job has no trigger (manual/API-run only) and is the only writer to the landing
# MAGIC Volume, which is what `sales_data_cc_pipeline`'s file-arrival trigger watches.

# COMMAND ----------

# DBTITLE 1,Widgets & catalog/schema/volume DDL
import re

dbutils.widgets.text("catalog", "sales_data_cc", "Catalog name")
dbutils.widgets.text("volume_name", "raw_files", "Volume name (for landed CSVs)")
dbutils.widgets.dropdown("seed_delta_files", "false", ["false", "true"], "Seed data/delta/ too?")

catalog = dbutils.widgets.get("catalog")
volume_name = dbutils.widgets.get("volume_name")
seed_delta_files = dbutils.widgets.get("seed_delta_files") == "true"

# Names are interpolated into SQL identifiers and Volume paths.
_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9_]+")
for _label, _value in (("catalog", catalog), ("volume_name", volume_name)):
    if not _IDENTIFIER_RE.fullmatch(_value):
        raise ValueError(
            f"Invalid {_label} '{_value}': must match {_IDENTIFIER_RE.pattern} "
            "(letters, digits, underscore only)."
        )

LANDING_SCHEMA = "landing"
volume_base = f"/Volumes/{catalog}/{LANDING_SCHEMA}/{volume_name}"

# Idempotent, minimal catalog/schema/volume DDL — only what seeding needs (no `load_mode`
# widget, unlike common/config.py's broader version used by the pipeline notebooks). Runs
# here too so seeding works even before the pipeline Job has created anything.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{LANDING_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{LANDING_SCHEMA}.{volume_name}")

print(f"catalog={catalog} | volume_name={volume_name} | seed_delta_files={seed_delta_files}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed the landing Volume from the bundled `data/` folder
# MAGIC `data/initial/*.csv` is copied once, idempotently — already-present files are
# MAGIC skipped, so re-running this task doesn't re-copy or re-trigger Auto Loader on them.
# MAGIC `data/delta/*.csv` is additionally copied only when `seed_delta_files=true`.
# MAGIC
# MAGIC `data_root` (the deployed bundle's own root folder) is derived from this notebook's
# MAGIC own workspace path — not a widget/parameter — since it's always the same two
# MAGIC directories up from wherever `bundle deploy` happened to place it
# MAGIC (`<bundle_root>/src/seed_landing_volume.py`), regardless of workspace, user, or
# MAGIC target.

# COMMAND ----------

# DBTITLE 1,Bundle-relative data_root
import os

_this_notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
data_root = f"{os.path.dirname(os.path.dirname(_this_notebook_path))}/data"
print(f"data_root={data_root}")

# COMMAND ----------

# DBTITLE 1,Seed helpers & run
def _existing_csv_names(path: str) -> set:
    """CSV filenames already present at path, or empty set if the path doesn't exist yet."""
    try:
        return {entry.name for entry in dbutils.fs.ls(path) if entry.name.lower().endswith(".csv")}
    except Exception:  # noqa: BLE001 - path simply may not exist yet
        return set()


def seed_from(local_subdir: str) -> list:
    """Idempotently copy new CSVs from data_root/local_subdir into volume_base, skipping
    any filename already present. Workspace Files are reachable from the driver's local
    filesystem at their /Workspace/... path, hence the 'file:' prefix for dbutils.fs."""
    source_dir = f"{data_root}/{local_subdir}"
    try:
        entries = dbutils.fs.ls(f"file:{source_dir}")
    except Exception as exc:  # noqa: BLE001 - surfaced with a clearer message below
        raise FileNotFoundError(
            f"Could not list '{source_dir}'. Was the bundle deployed with this folder "
            f"included (not excluded by sync_exclude/.gitignore)? Original error: {exc}"
        ) from exc

    already_present = _existing_csv_names(volume_base)
    copied = []
    for entry in entries:
        if not entry.name.lower().endswith(".csv") or entry.name in already_present:
            continue
        dbutils.fs.cp(f"file:{source_dir}/{entry.name}", f"{volume_base}/{entry.name}")
        copied.append(entry.name)
    return copied


initial_copied = seed_from("initial")
print(
    f"Seeded {len(initial_copied)} new baseline file(s): {initial_copied}"
    if initial_copied
    else "data/initial/ already present in the Volume — nothing to seed."
)

if seed_delta_files:
    delta_copied = seed_from("delta")
    print(
        f"Seeded {len(delta_copied)} new delta file(s): {delta_copied}"
        if delta_copied
        else "data/delta/ already present in the Volume — nothing to seed."
    )
