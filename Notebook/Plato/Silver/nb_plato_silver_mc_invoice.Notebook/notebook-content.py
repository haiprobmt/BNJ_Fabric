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

# # Plato Bronze → Silver: `invoice` → `silver_mc_invoice`

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = None
job_id = None              # required for logging (4-digit int/string is OK)
src_catalog = "plato_test"
job_group_name = "silver"
src_table = "invoice"
lh_silver_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/e8a5651c-a1b6-49de-b2c5-11b7e197e77a/Tables/plato_test'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# tgt_table = "silver_mc_invoice"
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
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Validate required params
if batch_id is None or str(batch_id).strip() == "":
    raise ValueError("Pipeline must pass batch_id (YYYYmmddhhmmss)")
if job_id is None or str(job_id).strip() == "":
    raise ValueError("Pipeline must pass job_id for this notebook")

job_id_str = str(job_id).strip()
src_table = "invoice"
tgt_table = "silver_mc_invoice"

# Start log
#start_job_instance(batch_id, job_id_str, msg=f"Start SILVER src_table={src_table} -> {tgt_table}")

result_payload = None

try:
    from pyspark.sql.functions import explode_outer, col

    # Source (Bronze)
    df = spark.sql("select * from lh_bnj_bronze.plato.invoice")

    # Target base path (must be defined)
    if not globals().get("lh_silver_path", None):
        raise ValueError("lh_silver_path is not defined. Pass it as notebook parameter or define it in nb_utils.")

    # -------------------------------------------------------------------
    # FIX: Bronze mc is STRING -> parse to ARRAY<STRUCT> before explode
    # -------------------------------------------------------------------
    mc_type = dict(df.dtypes).get("mc")  # 'string' or 'array<...>'
    print(f"[schema] mc_type={mc_type}")

    if mc_type == "string":
        mc_schema = ArrayType(
            StructType([
                StructField("cancel", StringType(), True),
                StructField("created_by", StringType(), True),
                StructField("created_on", StringType(), True),
                StructField("id", StringType(), True),
                StructField("serial", StringType(), True),
                StructField("text", StringType(), True),
                StructField("title", StringType(), True),
                StructField("others", StructType([
                    StructField("days", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("fromdate", StringType(), True),
                    StructField("fromemail", StringType(), True),
                    StructField("sendemail", StringType(), True),
                    StructField("sendsms", StringType(), True),
                    StructField("telephone", StringType(), True),
                    StructField("todate", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("digimc", StructType([
                        StructField("CreatedByName", StringType(), True),
                        StructField("CreatedWhen", StringType(), True),
                        StructField("DOB", StringType(), True),
                        StructField("DutyComment", StringType(), True),
                        StructField("FlagCourt", StringType(), True),
                        StructField("FlagDuration", StringType(), True),
                        StructField("FlagFit", StringType(), True),
                        StructField("FlagUnfit", StringType(), True),
                        StructField("Institution", StringType(), True),
                        StructField("MCType", StringType(), True),
                        StructField("MedicalCertificateID", StringType(), True),
                        StructField("PatientID", StringType(), True),
                        StructField("PatientName", StringType(), True),
                        StructField("ProviderMCR", StringType(), True),
                        StructField("Status", StringType(), True),
                        StructField("ToSend", StringType(), True),
                        StructField("UnfitFrom", StringType(), True),
                        StructField("UnfitTo", StringType(), True),
                        StructField("VisitID", StringType(), True),
                        StructField("Ward", StringType(), True),
                    ]), True),
                    StructField("response", StructType([
                        StructField("id", StringType(), True),
                        StructField("message", StringType(), True),
                        StructField("url", StringType(), True),
                    ]), True),
                ]), True),
            ])
        )

        df = df.withColumn(
            "mc_arr",
            F.when(F.col("mc").isNull() | (F.trim(F.col("mc")) == ""), F.array().cast(mc_schema))
             .otherwise(F.from_json(F.col("mc"), mc_schema))
        )
    else:
        # mc already array-like
        df = df.withColumn("mc_arr", F.col("mc"))

    # =============================================================================
    # STEP: EXPLODE AND FLATTEN MC (MEDICAL CERTIFICATE)
    # =============================================================================
    mc_df = (
        df.select(
            col("_id").alias("invoice_id"),
            explode_outer("mc_arr").alias("mc_detail")
        )
        .select(
            "invoice_id",
            col("mc_detail.cancel").alias("mc_cancel"),
            col("mc_detail.created_by").alias("mc_created_by"),
            col("mc_detail.created_on").alias("mc_created_on"),
            col("mc_detail.id").alias("mc_id"),
            col("mc_detail.serial").alias("mc_serial"),
            col("mc_detail.text").alias("mc_text"),
            col("mc_detail.title").alias("mc_title"),

            col("mc_detail.others.days").alias("mc_days"),
            col("mc_detail.others.email").alias("mc_email"),
            col("mc_detail.others.fromdate").alias("mc_fromdate"),
            col("mc_detail.others.fromemail").alias("mc_fromemail"),
            col("mc_detail.others.sendemail").alias("mc_sendemail"),
            col("mc_detail.others.sendsms").alias("mc_sendsms"),
            col("mc_detail.others.telephone").alias("mc_telephone"),
            col("mc_detail.others.todate").alias("mc_todate"),
            col("mc_detail.others.type").alias("mc_type"),

            col("mc_detail.others.digimc.CreatedByName").alias("digimc_created_by_name"),
            col("mc_detail.others.digimc.CreatedWhen").alias("digimc_created_when"),
            col("mc_detail.others.digimc.DOB").alias("digimc_dob"),
            col("mc_detail.others.digimc.DutyComment").alias("digimc_duty_comment"),
            col("mc_detail.others.digimc.FlagCourt").alias("digimc_flag_court"),
            col("mc_detail.others.digimc.FlagDuration").alias("digimc_flag_duration"),
            col("mc_detail.others.digimc.FlagFit").alias("digimc_flag_fit"),
            col("mc_detail.others.digimc.FlagUnfit").alias("digimc_flag_unfit"),
            col("mc_detail.others.digimc.Institution").alias("digimc_institution"),
            col("mc_detail.others.digimc.MCType").alias("digimc_mc_type"),
            col("mc_detail.others.digimc.MedicalCertificateID").alias("digimc_certificate_id"),
            col("mc_detail.others.digimc.PatientID").alias("digimc_patient_id"),
            col("mc_detail.others.digimc.PatientName").alias("digimc_patient_name"),
            col("mc_detail.others.digimc.ProviderMCR").alias("digimc_provider_mcr"),
            col("mc_detail.others.digimc.Status").alias("digimc_status"),
            col("mc_detail.others.digimc.ToSend").alias("digimc_to_send"),
            col("mc_detail.others.digimc.UnfitFrom").alias("digimc_unfit_from"),
            col("mc_detail.others.digimc.UnfitTo").alias("digimc_unfit_to"),
            col("mc_detail.others.digimc.VisitID").alias("digimc_visit_id"),
            col("mc_detail.others.digimc.Ward").alias("digimc_ward"),

            col("mc_detail.others.response.id").alias("mc_response_id"),
            col("mc_detail.others.response.message").alias("mc_response_message"),
            col("mc_detail.others.response.url").alias("mc_response_url"),
        )
    )

    out_path = f"{lh_silver_path}/{tgt_table}"

    mc_df.write.format("delta").mode("overwrite").save(out_path)

    # OPTIONAL: register table from path
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
    # Never treat NotebookExit as failure
    if e.__class__.__name__ == "NotebookExit":
        raise

    try:
        detail = safe_exception_text(e)
    except Exception:
        detail = str(e)

    end_job_instance(batch_id, job_id_str, "FAILED", msg=f"FAILED SILVER src_table={src_table} -> {tgt_table}. {detail}"[:8000])

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
