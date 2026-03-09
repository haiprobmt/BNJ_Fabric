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

# # Plato Bronze → Silver: `invoice` → `silver_corporate_invoice`

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

# tgt_table = "silver_corporate_invoice"
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
from pyspark.sql.functions import col, explode_outer, lit
from pyspark.sql.types import ArrayType, StructType

# Validate required params
if batch_id is None or str(batch_id).strip() == "":
    raise ValueError("Pipeline must pass batch_id (YYYYmmddhhmmss)")
if job_id is None or str(job_id).strip() == "":
    raise ValueError("Pipeline must pass job_id for this notebook")

job_id_str = str(job_id).strip()
src_table = "invoice"
tgt_table = "silver_corporate_invoice"

# Start log
#start_job_instance(batch_id, job_id_str, msg=f"Start SILVER src_table={src_table} -> {tgt_table}")

result_payload = None

try:
    # Source (Bronze)
    df = spark.sql("select * from lh_bnj_bronze.plato.invoice")

    # Validate target base path
    # if not lh_silver_path:
    #     raise ValueError("lh_silver_path is not defined. Pass it as notebook parameter or define it in nb_utils.")

    # -------------------------------
    # Detect corporate column type
    # -------------------------------
    corp_field = next((f for f in df.schema.fields if f.name == "corporate"), None)

    # Build a consistent output schema (so even empty writes create the expected columns)
    def empty_corporate_df():
        return spark.range(0).select(
            lit(None).cast("string").alias("invoice_id"),
            lit(None).cast("string").alias("corp_id"),
            lit(None).cast("decimal(18,2)").alias("corp_amount"),
            lit(None).cast("string").alias("corp_name"),
            lit(None).cast("string").alias("corp_created_by"),
            lit(None).cast("string").alias("corp_created_on"),
            lit(None).cast("string").alias("corp_invoice_id"),
            lit(None).cast("string").alias("corp_location"),
            lit(None).cast("string").alias("corp_ref"),
            lit(None).cast("string").alias("corp_void"),
            lit(None).cast("string").alias("corp_void_by"),
            lit(None).cast("string").alias("corp_void_on"),
            lit(None).cast("string").alias("corp_void_reason"),
        )

    if corp_field is None:
        # corporate column not present at all
        corporate_df = empty_corporate_df()

    else:
        corp_type = corp_field.dataType

        if isinstance(corp_type, ArrayType):
            # corporate is array<struct<...>>
            corporate_df = (
                df.select(
                    col("_id").alias("invoice_id"),
                    explode_outer(col("corporate")).alias("corporate_item")
                )
                .select(
                    "invoice_id",
                    col("corporate_item._id").alias("corp_id"),
                    col("corporate_item.amount").alias("corp_amount"),
                    col("corporate_item.corp").alias("corp_name"),
                    col("corporate_item.created_by").alias("corp_created_by"),
                    col("corporate_item.created_on").alias("corp_created_on"),
                    col("corporate_item.invoice_id").alias("corp_invoice_id"),
                    col("corporate_item.location").alias("corp_location"),
                    col("corporate_item.ref").alias("corp_ref"),
                    col("corporate_item.void").alias("corp_void"),
                    col("corporate_item.void_by").alias("corp_void_by"),
                    col("corporate_item.void_on").alias("corp_void_on"),
                    col("corporate_item.void_reason").alias("corp_void_reason"),
                )
            )

        elif isinstance(corp_type, StructType):
            # corporate is struct<...> (NO explode)
            corporate_df = df.select(
                col("_id").alias("invoice_id"),
                col("corporate._id").alias("corp_id"),
                col("corporate.amount").alias("corp_amount"),
                col("corporate.corp").alias("corp_name"),
                col("corporate.created_by").alias("corp_created_by"),
                col("corporate.created_on").alias("corp_created_on"),
                col("corporate.invoice_id").alias("corp_invoice_id"),
                col("corporate.location").alias("corp_location"),
                col("corporate.ref").alias("corp_ref"),
                col("corporate.void").alias("corp_void"),
                col("corporate.void_by").alias("corp_void_by"),
                col("corporate.void_on").alias("corp_void_on"),
                col("corporate.void_reason").alias("corp_void_reason"),
            )

        else:
            # Unexpected type (string, etc.) -> write empty but keep pipeline stable
            corporate_df = empty_corporate_df()

    out_path = f"{lh_silver_path}/{tgt_table}"
    corporate_df.write.format("delta").mode("overwrite").save(out_path)

    # OPTIONAL: register table from path
    # spark.sql("CREATE DATABASE IF NOT EXISTS lh_bnj_silver.plato_test")
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
    # Never treat NotebookExit as failure
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

    raise Exception(json.dumps({
        "return_code": -1,
        "return_msg": "FAILED",
        "batch_id": batch_id,
        "job_id": job_id_str,
        "src_table": src_table,
        "tgt_table": tgt_table,
        "error": detail[:8000]
    }, ensure_ascii=False))

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
