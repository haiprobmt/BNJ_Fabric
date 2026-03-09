# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "48bd1f5e-ef56-4df0-8515-17758bcbd734",
# META       "default_lakehouse_name": "lh_bnj_metadata",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import col
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F
import re

csv_path  = "abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/48bd1f5e-ef56-4df0-8515-17758bcbd734/Files/BNJ_etl_jobs.csv"
tgt_table = "lh_bnj_metadata.md.etl_jobs"

BNJ_COLS = [
    "job_id",
    "job_group_name",
    "job_name",
    "active_flg",
    "src_catalog",
    "src_table",
    "tgt_catalog",
    "tgt_table",
    "job_script",
    "filter_condition",
    "dynamic_integration_id",
    "created_dt",
    "last_updated_dt",
    "batch_group",
]

csv_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("ignoreLeadingWhiteSpace", "true")
    .option("ignoreTrailingWhiteSpace", "true")
    .load(csv_path)
)

def normalize_col(c: str) -> str:
    c = c.strip().lower()
    c = re.sub(r"[^\w]+", "_", c)
    c = re.sub(r"_+", "_", c).strip("_")
    return c

csv_df = csv_df.select(*[F.col(c).alias(normalize_col(c)) for c in csv_df.columns])

for c in BNJ_COLS:
    if c not in csv_df.columns:
        csv_df = csv_df.withColumn(c, F.lit(None))

csv_df = csv_df.select(*BNJ_COLS)

def nz(colname: str):
    return F.coalesce(F.col(colname).cast("string"), F.lit(""))

computed_job_id = (
    F.pmod(
        F.xxhash64(
            nz("batch_group"),
            nz("src_catalog"),
            nz("src_table"),
            nz("tgt_catalog"),
            nz("tgt_table"),
        ),
        F.lit(9000)
    ) + F.lit(1000)
).cast("bigint")

csv_df = csv_df.withColumn(
    "job_id",
    F.when(
        F.col("job_id").isNull() | (F.trim(F.col("job_id").cast("string")) == ""),
        computed_job_id
    ).otherwise(F.col("job_id").cast("bigint"))
)

target_exists = True
try:
    tgt_df = spark.table(tgt_table)
except Exception:
    target_exists = False

def parse_ts(col_expr):
    """Parse timestamp from various common string formats."""
    return F.coalesce(
        F.to_timestamp(col_expr),  # Spark default
        F.to_timestamp(col_expr, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(col_expr, "yyyy-MM-dd"),
    )

if target_exists:
    tgt_schema = {f.name.lower(): f.dataType for f in tgt_df.schema}
    tgt_cols_lower = [f.name.lower() for f in tgt_df.schema]

    # Add any columns that exist in target but not in csv_df
    for c in tgt_cols_lower:
        if c not in csv_df.columns:
            csv_df = csv_df.withColumn(c, F.lit(None))

    # Cast columns to target types (special handling for timestamps)
    for c, dtype in tgt_schema.items():
        if c in csv_df.columns:
            if "timestamp" in dtype.simpleString():
                csv_df = csv_df.withColumn(c, parse_ts(F.col(c)))
            else:
                csv_df = csv_df.withColumn(c, F.col(c).cast(dtype))

    csv_df = (
        csv_df
        .withColumn("created_dt", F.coalesce(F.col("created_dt"), F.current_timestamp()))
        .withColumn("last_updated_dt", F.current_timestamp())
    )

    # Reorder to exactly match target table columns
    final_df = csv_df.select(*tgt_cols_lower)

    (
        final_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "false")
        .saveAsTable(tgt_table)
    )

else:
    # Target table not found - basic casting so the table can be created
    csv_df = (
        csv_df
        .withColumn("job_id", F.col("job_id").cast("bigint"))
        .withColumn("created_dt", parse_ts(F.col("created_dt")))
        .withColumn("last_updated_dt", parse_ts(F.col("last_updated_dt")))
    )

    # Apply your timestamp rules for new table creation too
    final_df = (
        csv_df
        .withColumn("created_dt", F.coalesce(F.col("created_dt"), F.current_timestamp()))
        .withColumn("last_updated_dt", F.current_timestamp())
    )

    (
        final_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tgt_table)
    )

print("BNJ CSV loaded and overwritten successfully into:", tgt_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select * from md.etl_jobs
# MAGIC 
# MAGIC -- please load data with Spark(Python) or Spark(R)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/48bd1f5e-ef56-4df0-8515-17758bcbd734/Files/BNJ_etl_jobs.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/48bd1f5e-ef56-4df0-8515-17758bcbd734/Files/BNJ_etl_jobs.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
