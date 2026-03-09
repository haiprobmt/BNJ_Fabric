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
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         },
# META         {
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
# META         },
# META         {
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Plato Bronze → Silver: `invoice` → `silver_base_invoice`

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = None
job_id = None              # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "invoice"
lh_silver_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/e8a5651c-a1b6-49de-b2c5-11b7e197e77a/Tables/plato_test'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# tgt_table = "silver_base_invoice"
# out_path = f"{lh_silver_path}/{tgt_table}"

# # delete the whole delta folder (data + _delta_log)
# if mssparkutils.fs.exists(out_path):
#     mssparkutils.fs.rm(out_path, True)
#     print(f"✅ Deleted: {out_path}")
# else:
#     print(f"⏭️ Not found: {out_path}")


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

import json
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# Validate required params
if batch_id is None or str(batch_id).strip() == "":
    raise ValueError("Pipeline must pass batch_id (YYYYmmddhhmmss)")
if job_id is None or str(job_id).strip() == "":
    raise ValueError("Pipeline must pass job_id for this notebook")

job_id_str = str(job_id).strip()
src_table = "invoice"
tgt_table = "silver_base_invoice"

# Target base path (must exist in your environment)
# if not globals().get("lh_silver_path", None):
#     raise ValueError("lh_silver_path is not defined. Pass it as notebook parameter or define it in nb_utils.")

# Start log
#start_job_instance(batch_id, job_id_str, msg=f"Start SILVER src_table={src_table} -> {tgt_table}")

result_payload = None

try:
    # Source (Bronze)
    df = spark.sql("select * from lh_bnj_bronze.plato.invoice")

    base_invoice_df = df.select(
        col("_id").alias("invoice_id"),
        col("adj"),
        col("adj_amount"),
        col("cndn"),
        col("cndn_apply_to"),
        col("corp_notes"),
        col("created_by"),
        col("created_on"),
        col("date"),
        col("doctor"),
        col("finalized"),
        col("finalized_by"),
        col("finalized_on"),
        col("highlight"),
        col("invoice"),
        col("invoice_notes"),
        col("invoice_prefix"),
        col("last_edited"),
        col("last_edited_by"),
        col("location"),
        col("manual_timein"),
        col("manual_timeout"),
        col("no_gst"),
        col("notes"),
        col("patient_id"),
        col("rate"),
        col("scheme"),
        col("session"),
        col("status"),
        col("status_on"),
        col("sub_total"),
        col("tax"),
        col("total"),
        col("void"),
        col("void_by"),
        col("void_on"),
        col("void_reason")
    )

    out_path = f"{lh_silver_path}/{tgt_table}"
    base_invoice_df.write.format("delta").mode("overwrite").save(out_path)

    # OPTIONAL: register table from path (adjust db/schema as needed)
    # spark.sql("CREATE DATABASE IF NOT EXISTS lh_bnj_silver.plato")
    # spark.sql(f"""
    # CREATE TABLE IF NOT EXISTS lh_bnj_silver.plato_test.{tgt_table}
    # USING DELTA
    # LOCATION '{out_path}'
    # """)

    # Success log
    row_cnt = None
    try:
        if not False:
            row_cnt = spark.read.format("delta").load(out_path).count()
    except Exception:
        row_cnt = None

    end_job_instance(batch_id, job_id_str, "SUCCESS",
                     msg=f"Completed SILVER src_table={src_table} -> {tgt_table} rows={row_cnt}")

    result_payload = {
        "return_code": 0,
        "return_msg": "OK",
        "batch_id": batch_id,
        "job_id": job_id_str,
        "src_table": src_table,
        "tgt_table": tgt_table,
        "rows_written": row_cnt
    }

except Exception as e:
    # IMPORTANT: mssparkutils.notebook.exit raises NotebookExit internally.
    # Never treat it as failure.
    if e.__class__.__name__ == "NotebookExit":
        raise

    try:
        detail = safe_exception_text(e)
    except Exception:
        detail = str(e)

    end_job_instance(
        batch_id, job_id_str, "FAILED",
        msg=f"FAILED SILVER src_table={src_table} -> {tgt_table}. {detail}"[:8000]
    )

    # Raise so pipeline activity becomes Failed
    raise Exception(json.dumps({
        "return_code": -1,
        "return_msg": "FAILED",
        "batch_id": batch_id,
        "job_id": job_id_str,
        "src_table": src_table,
        "tgt_table": tgt_table,
        "error": detail[:8000]
    }, ensure_ascii=False))

# Exit AFTER try/except so NotebookExit is not caught and mis-logged as FAILED
mssparkutils.notebook.exit(json.dumps(result_payload, ensure_ascii=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
