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
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         },
# META         {
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
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

# # Gold: dim_payment_mode


# PARAMETERS CELL ********************

# ==========================================
# 0) Pipeline parameters
# ==========================================
batch_id = None
job_id = None
src_catalog = "plato_test"
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
SILVER_SCHEMA = f"lh_bnj_silver.plato_test"
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
src_table = "silver_payment_mode"
tgt_table = "dim_payment_mode"

# -----------------------------
# Table transformation function
# -----------------------------

def load_dim_payment_mode():

    silver_payment = spark.table(f"{SILVER_SCHEMA}.silver_payment")

    # Get distinct payment modes
    payment_modes = silver_payment.select('payment_mode').distinct().filter(col('payment_mode').isNotNull())

    # Add row number for surrogate key
    window = Window.orderBy('payment_mode')

    dim_payment_mode = payment_modes.withColumn('payment_mode_key', row_number().over(window)).select(
        col('payment_mode_key'),
        col('payment_mode').alias('payment_mode_code'),
        col('payment_mode').alias('payment_mode_name'),

        # Categorize payment modes
        when(col('payment_mode').isin(['CASH', 'Cash']), 'Cash')
        .when(col('payment_mode').rlike('(?i)card|credit|debit|visa|master'), 'Card')
        .when(col('payment_mode').rlike('(?i)paynow|grabpay|nets|bank|transfer'), 'Digital')
        .when(col('payment_mode').rlike('(?i)insurance|medisave|medishield'), 'Insurance')
        .when(col('payment_mode').rlike('(?i)corporate|company'), 'Corporate')
        .otherwise('Other').alias('payment_mode_category'),

        # Attributes
        when(col('payment_mode').rlike('(?i)card|bank|transfer|cheque'), True).otherwise(False).alias('requires_reference'),
        when(col('payment_mode').rlike('(?i)card|paynow|grabpay|nets|bank|transfer'), True).otherwise(False).alias('is_electronic'),
        lit(True).alias('is_active'),

        # Audit fields
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )

    # Write to Delta table
    dim_payment_mode.write \
        .mode('overwrite') \
        .format('delta') \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{GOLD_SCHEMA}.dim_payment_mode")

    row_count = dim_payment_mode.count()
    print(f"✅ DIM_PAYMENT_MODE created with {row_count:,} rows")
    return dim_payment_mode

# -----------------------------
# Logging: start
# -----------------------------
start_job_instance(batch_id, job_id_str, msg=f"Start GOLD src_table={src_table} -> {GOLD_SCHEMA}.{tgt_table}")

result_payload = None

try:
    df = load_dim_payment_mode()
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
