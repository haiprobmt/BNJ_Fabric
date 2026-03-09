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

# # Plato Bronze → Silver: `invoice` → `silver_prescription_invoice`

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

# tgt_table = "silver_prescription_invoice"
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
from pyspark.sql.functions import col, explode_outer, lit, when, from_json
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# -----------------------------
# Validate required params
# -----------------------------
if batch_id is None or str(batch_id).strip() == "":
    raise ValueError("Pipeline must pass batch_id (YYYYmmddhhmmss)")
if job_id is None or str(job_id).strip() == "":
    raise ValueError("Pipeline must pass job_id for this notebook")

job_id_str = str(job_id).strip()
src_table  = "invoice"
tgt_table  = "silver_prescription_invoice"

# Target base path
lh_silver_path = globals().get("lh_silver_path", None)
if not lh_silver_path:
    raise ValueError("lh_silver_path is not defined. Pass it as notebook parameter or define it in nb_utils.")

# -----------------------------
# Logging: start
# -----------------------------
#start_job_instance(batch_id, job_id_str, msg=f"Start SILVER src_table={src_table} -> {tgt_table}")

result_payload = None

try:
    # Source (Bronze)
    df = spark.sql("select * from lh_bnj_bronze.plato.invoice")

    # -------------------------------
    # Helpers: consistent empty output
    # -------------------------------
    def empty_prescription_df():
        return spark.range(0).select(
            lit(None).cast("string").alias("invoice_id"),
            lit(None).cast("string").alias("prescription_cancel"),
            lit(None).cast("string").alias("prescription_created_by"),
            lit(None).cast("string").alias("prescription_created_on"),
            lit(None).cast("string").alias("prescription_id"),
            lit(None).cast("string").alias("prescription_others"),
            lit(None).cast("string").alias("prescription_serial"),
            lit(None).cast("string").alias("med_dosage"),
            lit(None).cast("string").alias("med_name"),
        )

    # -------------------------------
    # Detect prescription column type
    # -------------------------------
    pres_field = next((f for f in df.schema.fields if f.name == "prescription"), None)

    # If column missing: produce empty
    if pres_field is None:
        prescription_df = empty_prescription_df()

    else:
        pres_type = pres_field.dataType

        # If STRING: attempt JSON parse into array<struct<...>>
        if isinstance(pres_type, StringType):
            prescription_schema = ArrayType(
                StructType([
                    StructField("cancel", StringType(), True),
                    StructField("created_by", StringType(), True),
                    StructField("created_on", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("others", StringType(), True),
                    StructField("serial", StringType(), True),
                    StructField("meds", ArrayType(
                        StructType([
                            StructField("dosage", StringType(), True),
                            StructField("name", StringType(), True),
                        ])
                    ), True),
                ])
            )

            df = df.withColumn(
                "prescription",
                when(col("prescription").isNotNull(), from_json(col("prescription"), prescription_schema)).otherwise(lit(None))
            )
            pres_type = ArrayType(StructType())  # treat as array after parse

        # Re-detect after parsing (or original type)
        pres_field2 = next((f for f in df.schema.fields if f.name == "prescription"), None)
        pres_type2 = pres_field2.dataType if pres_field2 else None

        if isinstance(pres_type2, ArrayType):
            # prescription is array<struct<...>>
            prescription_exploded = (
                df.select(
                    col("_id").alias("invoice_id"),
                    explode_outer(col("prescription")).alias("prescription_detail")
                )
                .select(
                    "invoice_id",
                    col("prescription_detail.cancel").alias("prescription_cancel"),
                    col("prescription_detail.created_by").alias("prescription_created_by"),
                    col("prescription_detail.created_on").alias("prescription_created_on"),
                    col("prescription_detail.id").alias("prescription_id"),
                    col("prescription_detail.others").alias("prescription_others"),
                    col("prescription_detail.serial").alias("prescription_serial"),
                    col("prescription_detail.meds").alias("meds"),
                )
            )

            prescription_df = (
                prescription_exploded
                .select(
                    "invoice_id",
                    "prescription_cancel",
                    "prescription_created_by",
                    "prescription_created_on",
                    "prescription_id",
                    "prescription_others",
                    "prescription_serial",
                    explode_outer(col("meds")).alias("med_detail")
                )
                .select(
                    "invoice_id",
                    "prescription_cancel",
                    "prescription_created_by",
                    "prescription_created_on",
                    "prescription_id",
                    "prescription_others",
                    "prescription_serial",
                    col("med_detail.dosage").alias("med_dosage"),
                    col("med_detail.name").alias("med_name"),
                )
            )

        elif isinstance(pres_type2, StructType):
            # prescription is struct<...> (single object)
            # explode meds if it exists and is array, else output one row
            pres_base = df.select(
                col("_id").alias("invoice_id"),
                col("prescription.cancel").alias("prescription_cancel"),
                col("prescription.created_by").alias("prescription_created_by"),
                col("prescription.created_on").alias("prescription_created_on"),
                col("prescription.id").alias("prescription_id"),
                col("prescription.others").alias("prescription_others"),
                col("prescription.serial").alias("prescription_serial"),
                col("prescription.meds").alias("meds"),
            )

            meds_field = None
            # If meds is nested under struct, we can only infer via schema if present
            # Try to explode; if it's not array, explode_outer will throw -> handle via conditional
            try:
                prescription_df = (
                    pres_base
                    .select(
                        "invoice_id",
                        "prescription_cancel",
                        "prescription_created_by",
                        "prescription_created_on",
                        "prescription_id",
                        "prescription_others",
                        "prescription_serial",
                        explode_outer(col("meds")).alias("med_detail")
                    )
                    .select(
                        "invoice_id",
                        "prescription_cancel",
                        "prescription_created_by",
                        "prescription_created_on",
                        "prescription_id",
                        "prescription_others",
                        "prescription_serial",
                        col("med_detail.dosage").alias("med_dosage"),
                        col("med_detail.name").alias("med_name"),
                    )
                )
            except Exception:
                # meds not explodable -> single row per invoice
                prescription_df = pres_base.select(
                    "invoice_id",
                    "prescription_cancel",
                    "prescription_created_by",
                    "prescription_created_on",
                    "prescription_id",
                    "prescription_others",
                    "prescription_serial",
                    lit(None).cast("string").alias("med_dosage"),
                    lit(None).cast("string").alias("med_name"),
                )

        else:
            # Unexpected datatype (or null-only) -> empty output
            prescription_df = empty_prescription_df()

    # -----------------------------
    # Write + register
    # -----------------------------
    out_path = f"{lh_silver_path}/{tgt_table}"
    prescription_df.write.format("delta").mode("overwrite").save(out_path)

    # spark.sql("CREATE DATABASE IF NOT EXISTS lh_bnj_silver.plato_test")
    # spark.sql(f"""
    # CREATE TABLE IF NOT EXISTS lh_bnj_silver.plato_test.{tgt_table}
    # USING DELTA
    # LOCATION '{out_path}'
    # """)

    # -----------------------------
    # Logging: success
    # -----------------------------
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
