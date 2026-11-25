# Databricks notebook source
# MAGIC %md
# MAGIC # Shared config
# MAGIC Widgets, catalog/schema/volume provisioning, and path/name helpers common to every
# MAGIC notebook in the `sales_data_cc_pipeline` Job. Loaded through `%run` by
# MAGIC `01_bronze_ingestion.py`, `02_silver_transform.py`, and `03_gold_aggregations.py` —
# MAGIC each gets its own copy of `catalog`/`volume_name`/`load_mode`/`volume_base`/etc. and
# MAGIC the idempotent `CREATE ... IF NOT EXISTS` statements re-run harmlessly every time.
# COMMAND ----------

# DBTITLE 1,Widgets & configuration
import re

# Pin the session timezone so timestamp arithmetic is deterministic
# regardless of cluster/driver defaults; inherited by %run dependents.
spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.text("catalog", "sales_data_cc", "Catalog name")
dbutils.widgets.text("volume_name", "raw_files", "Volume name (for landed CSVs)")
dbutils.widgets.dropdown("load_mode", "delta", ["initial", "delta"], "Load mode")

catalog = dbutils.widgets.get("catalog")
volume_name = dbutils.widgets.get("volume_name")
load_mode = dbutils.widgets.get("load_mode")

# Names are interpolated into SQL identifiers and Volume paths.
_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9_]+")
for _label, _value in (("catalog", catalog), ("volume_name", volume_name)):
    if not _IDENTIFIER_RE.fullmatch(_value):
        raise ValueError(
            f"Invalid {_label} '{_value}': must match {_IDENTIFIER_RE.pattern} "
            "(letters, digits, underscore only)."
        )

LANDING_SCHEMA = "landing"
CHECKPOINT_VOLUME = "checkpoints"
volume_base = f"/Volumes/{catalog}/{LANDING_SCHEMA}/{volume_name}"
checkpoint_base = f"/Volumes/{catalog}/{LANDING_SCHEMA}/{CHECKPOINT_VOLUME}"

print(f"catalog={catalog} | volume_name={volume_name} | load_mode={load_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC Catalog / schemas / volume Requires Unity Catalog creation privileges; Quarantine has a separate schema for rejected rows.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
for layer_schema in (LANDING_SCHEMA, "bronze", "silver", "quarantine", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{layer_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{LANDING_SCHEMA}.{volume_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{LANDING_SCHEMA}.{CHECKPOINT_VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader state paths
# MAGIC Each Bronze source gets its own checkpoint/schema directory.

# COMMAND ----------

def checkpoint_path(entity: str) -> str:
    """Per-source Auto Loader checkpoint directory, e.g. for products_raw."""
    return f"{checkpoint_base}/{entity}/checkpoint"


def schema_path(entity: str) -> str:
    """Per-source Auto Loader inferred-schema directory."""
    return f"{checkpoint_base}/{entity}/schema"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table naming

# COMMAND ----------

def table_name(layer: str, entity: str) -> str:
    """Build a fully-qualified table name, e.g. table_name("silver", "products")."""
    return f"{catalog}.{layer}.{entity}"


print(table_name("bronze", "products_raw"))
print(volume_base)
