# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "102823e0-12f1-4ca5-b61b-a2df5d75beb2",
# META       "default_lakehouse_name": "lh_bnj_gold",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
# META         },
# META         {
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         },
# META         {
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         },
# META         {
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold: dim_corporate


# PARAMETERS CELL ********************

# ==========================================
# 0) Pipeline parameters
# ==========================================
batch_id = None
job_id = None
src_catalog = "plato"
job_group_name = "gold"
tgt_catalog = "gold"

# ==========================================
# 2) Notebook-specific parameters
# ==========================================
# For DIM_DATE only; ignored by other notebooks
start_date = "2020-01-01"
end_date   = "2030-12-31"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# 1) Environment (override if needed)
# ==========================================
SILVER_SCHEMA = f"lh_bnj_silver.{src_catalog}"
GOLD_SCHEMA   = f"lh_bnj_gold.{tgt_catalog}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_utils


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# 3) Imports
# ==========================================
import json
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

print("✅ Environment configured")
print(f"Source: {SILVER_SCHEMA}")
print(f"Target: {GOLD_SCHEMA}")
print(f"[params] batch_id={batch_id}, job_id={job_id}, src_catalog={src_catalog}, job_group_name={job_group_name}")

# -----------------------------
# Validate required params
# -----------------------------
if batch_id is None or str(batch_id).strip() == "":
    raise ValueError("Pipeline must pass batch_id (YYYYmmddhhmmss)")
if job_id is None or str(job_id).strip() == "":
    raise ValueError("Pipeline must pass job_id for this notebook")

job_id_str = str(job_id).strip()
src_table = "silver_corporate"
tgt_table = "dim_corporate"

# -----------------------------
# Table transformation function
# -----------------------------

def load_dim_corporate():

    silver_corporate = spark.table(f"{SILVER_SCHEMA}.silver_corporate")

    dim_corporate = silver_corporate.select(
        abs(hash(concat(col('corporate_id'), coalesce(col('last_edited'), col('created_on'))))).alias('corporate_key'),

        # Natural key
        col('corporate_id'),

        # Corporate details
        col('given_id'),
        col('corporate_name'),
        col('category'),
        col('is_insurance'),

        # Contact information
        col('contact_person'),
        col('email'),
        col('telephone'),
        col('fax'),
        col('address'),
        col('url'),

        # Payment terms
        col('default_amount'),
        col('payment_type'),
        col('scheme'),

        # Policy details (JSON)
        col('discounts_json'),
        col('specials_json'),
        col('restricted_json'),

        # SCD Type 2 attributes
        coalesce(col('created_on'), current_timestamp()).alias('effective_date'),
        lit(None).cast('timestamp').alias('end_date'),
        lit(True).alias('is_current'),

        # Audit fields
        lit('PLATO').alias('source_system'),
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    dim_corporate = dim_corporate.withColumn("corporate_key", col("corporate_key").cast("long"))
    # Write to Delta table
    dim_corporate.write \
        .mode('overwrite') \
        .format('delta') \
        .option("overwriteSchema", "true") \
        .partitionBy('is_insurance', 'is_current') \
        .saveAsTable(f"{GOLD_SCHEMA}.dim_corporate")

    row_count = dim_corporate.count()
    print(f"✅ DIM_CORPORATE created with {row_count:,} rows")
    return dim_corporate

# -----------------------------
# Logging: start
# -----------------------------
start_job_instance(batch_id, job_id_str, msg=f"Start GOLD src_table={src_table} -> {GOLD_SCHEMA}.{tgt_table}")

result_payload = None

try:
    df = load_dim_corporate()
    # best-effort row count (some functions already count/print)
    try:
        row_cnt = df.count()
    except Exception:
        row_cnt = None

    end_job_instance(batch_id, job_id_str, "SUCCESS",
                        msg=f"SUCCESS GOLD src_table={src_table} -> {GOLD_SCHEMA}.{tgt_table} rows={row_cnt}")

    result_payload = {
        "return_code": 0,
        "return_msg": "OK",
        "batch_id": str(batch_id),
        "job_id": str(job_id_str),
        "src_table": src_table,
        "tgt_table": f"{GOLD_SCHEMA}.{tgt_table}",
        "rows_written": row_cnt
    }

except Exception as e:
    try:
        detail = safe_exception_text(e)  # if provided by nb_utils
    except Exception:
        detail = traceback.format_exc()

    end_job_instance(batch_id, job_id_str, "FAILED",
                        msg=f"FAILED GOLD src_table={src_table} -> {GOLD_SCHEMA}.{tgt_table}. {str(e)}"[:8000])

    raise Exception(json.dumps({
        "return_code": -1,
        "return_msg": "FAILED",
        "batch_id": str(batch_id),
        "job_id": str(job_id_str),
        "src_table": src_table,
        "tgt_table": f"{GOLD_SCHEMA}.{tgt_table}",
        "error": str(e)[:2000],
        "detail": detail[:8000]
    }, ensure_ascii=False))

mssparkutils.notebook.exit(json.dumps(result_payload, ensure_ascii=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
