# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a",
# META       "default_lakehouse_name": "lh_bnj_silver",
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

# CELL ********************

# ============================================
# 0) Notebook parameters
# ============================================
# Expected pipeline notebook parameters:
#   batch_id: string (YYYYmmddhhmmss)
#   input_dir: ABFSS folder containing Plato JSON files (one file per domain), e.g. .../Files/plato_dev
#   bronze_catalog: Spark catalog for Bronze lakehouse (e.g., 'lh_bnj_bronze')
#
# Job source:
# - Reads jobs from table: md.etl_jobs
# - Filters: job_group_name = 'bronze', active_flg='Y', src_catalog='plato'
#
# Notes:
# - job_id must be 4 digits for logging; we zfill/lpad to 4 digits.
# - JSON file name convention assumed: <input_dir>/<job_name>.json
#   (If your extractor writes <job_id>_<job_name>.json, adjust json_path below.)


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

# ============================================
# 2) Read bronze jobs from md.etl_jobs
# ============================================
from pyspark.sql import functions as F

jobs_raw = spark.table("lh_bnj_metadata.md.etl_jobs")

jobs_bronze = (
    jobs_raw
    .withColumn("job_group_name_lc", F.lower(F.col("job_group_name")))
    .withColumn("active_flg_uc", F.upper(F.col("active_flg")))
    .withColumn("src_catalog_lc", F.lower(F.col("src_catalog")))
    .filter(
        (F.col("job_group_name_lc") == "bronze") &
        (F.col("active_flg_uc") == "Y") &
        (F.col("src_catalog_lc") == "plato")
    )
    .select("job_id", "job_name", "src_table", "tgt_catalog", "tgt_table")
)

# Normalize job_id to 4 chars (supports INT or STRING)
jobs_bronze = (
    jobs_bronze
    .withColumn("job_id_str", F.col("job_id").cast("string"))
    .withColumn("job_id_effective", F.lpad(F.regexp_replace(F.col("job_id_str"), r"\.0$", ""), 4, "0"))
)

# Build the target full table name: <bronze_catalog>.<tgt_catalog>.<tgt_table>
# Example: lh_bnj_bronze.plato.brz_invoice
jobs_bronze = jobs_bronze.withColumn(
    "tgt_full_table",
    F.concat_ws(".", F.lit('lh_bnj_bronze'), F.col("tgt_catalog"), F.col("tgt_table"))
)

display(jobs_bronze.select("job_id_effective", "job_name", "src_table", "tgt_full_table").orderBy("job_id_effective"))
print("Bronze jobs:", jobs_bronze.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 3) Execute bronze loads + log per job
# ============================================
import json

# Optional: allow schema evolution on append
try:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
except Exception:
    pass

# RUN header
log_run_start(batch_id, msg="Plato bronze load started")

failed_any = False
failed_details = []

jobs = (
    jobs_bronze
    .select("job_id_effective", "job_name", "src_table", "tgt_full_table")
    .orderBy("job_id_effective")
    .collect()
)

for r in jobs:
    job_id = str(r["job_id_effective"])
    domain = str(r["job_name"]).strip()
    tgt_full_table = str(r["tgt_full_table"]).strip()

    if not domain:
        continue

    # If your extractor writes file name as "<job_id>_<domain>.json", change to:
    # json_path = f"{input_dir}/{job_id}_{domain}.json"
    json_path = f"{input_dir}/{domain}.json"

    log_step_start(batch_id, job_id, msg=f"Start bronze load: domain={domain}, json={json_path}, tgt={tgt_full_table}")
    print(f"[{job_id}] Bronze load domain={domain} -> {tgt_full_table}")

    try:
        df = (
            spark.read.format("json")
            .option("multiLine", "true")
            .load(json_path)
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.lit(json_path))
        )

        src_cnt = df.count()

        # Append to bronze table
        (df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(tgt_full_table)
        )

        # For append, tgt rows appended ~= src rows (we log src_cnt as tgt_cnt)
        log_step_end(
            batch_id, job_id, "SUCCESS",
            msg=f"Bronze load success: domain={domain}, appended_rows={src_cnt}, tgt={tgt_full_table}",
            src_row_num=src_cnt,
            tgt_row_num=src_cnt
        )

        print(f"[{job_id}] SUCCESS {domain}. Rows={src_cnt}")

    except Exception as e:
        failed_any = True

        log_step_end(
            batch_id, job_id, "FAILED",
            msg=f"Bronze load failed: domain={domain}. {safe_exception_text(e)}"
        )

        failed_details.append({"job_id": job_id, "domain": domain, "error": str(e)[:1500]})
        print(f"[{job_id}] FAILED {domain}: {e}")

        # Fail fast (stop the pipeline)
        break

# RUN footer + notebook result
if failed_any:
    log_run_end(batch_id, "FAILED", msg="Plato bronze load finished with failures")
    payload = {"return_code": -1, "return_msg": "FAILED", "batch_id": batch_id, "failed_details": failed_details}
    raise Exception(json.dumps(payload, ensure_ascii=False))
else:
    log_run_end(batch_id, "SUCCESS", msg="Plato bronze load finished successfully")
    mssparkutils.notebook.exit(json.dumps({"return_code": 0, "return_msg": "OK", "batch_id": batch_id}, ensure_ascii=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
