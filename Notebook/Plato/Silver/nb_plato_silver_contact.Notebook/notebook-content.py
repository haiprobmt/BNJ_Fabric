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

# # Plato Bronze → Silver: `contact` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **contact** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260204173700
job_id = 8214              # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "brz_contact"
lh_silver_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/e8a5651c-a1b6-49de-b2c5-11b7e197e77a/Tables/plato_test'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# tgt_table = "silver_contact"
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

print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import traceback

if batch_id is None or str(batch_id).strip() == "":
    raise ValueError("Pipeline must pass batch_id (YYYYmmddhhmmss)")
if job_id is None or str(job_id).strip() == "":
    raise ValueError("Pipeline must pass job_id for this notebook")

job_id_str = str(job_id).strip()
src_table = "brz_contact"
tgt_table = "silver_contact"

SILVER_SCHEMA = f"lh_bnj_silver.{src_catalog}"
BRONZE_SCHEMA = f"lh_bnj_bronze.{src_catalog}"

if 'lh_silver_path' not in globals() or lh_silver_path is None or str(lh_silver_path).strip() == "":
    raise ValueError("lh_silver_path is not defined. Pass it as notebook parameter.")

#start_job_instance(batch_id, job_id_str, msg=f"Start SILVER src_table={src_table} -> {tgt_table}")

result_payload = None

final_query = f"""
            SELECT
                -- Primary Key
                TRIM(_id) AS contact_id,

                -- Basic Information
                TRIM(name) AS contact_name,
                TRIM(person) AS person_name,
                TRIM(category) AS category,

                -- Contact Information
                TRIM(email) AS email,
                TRIM(handphone) AS handphone,
                TRIM(telephone) AS telephone,
                TRIM(fax) AS fax,
                NULLIF(TRIM(address), '') AS address,
                TRIM(url) AS url,

                -- Notes
                NULLIF(TRIM(notes), '') AS notes,

                -- Audit Fields
                TRY_CAST(created_on AS TIMESTAMP) AS created_on,
                TRIM(created_by) AS created_by,
                TRY_CAST(last_edited AS TIMESTAMP) AS last_edited,
                TRIM(last_edited_by) AS last_edited_by,

                -- Metadata
                CURRENT_TIMESTAMP() AS silver_loaded_at
            FROM {BRONZE_SCHEMA}.{src_table}
            WHERE _id IS NOT NULL;
            """

try:
    out_path = f"{lh_silver_path}/silver_contact"


    # Transform (from nb_plato_bronze_to_silver)
    # contact
    df_final = spark.sql(final_query)
    # df_final.printSchema()
    df_final.write.format('delta').mode('overwrite').save(f'{lh_silver_path}/silver_contact')


    # Ensure clean overwrite (avoid Delta schema merge issues at existing path)
    try:
        if mssparkutils.fs.exists(out_path):
            mssparkutils.fs.rm(out_path, True)
    except Exception:
        pass

    # Register table to Lakehouse metastore (idempotent)
    # spark.sql(f"CREATE DATABASE IF NOT EXISTS lh_bnj_silver.{src_catalog}")
    # spark.sql(f"DROP TABLE IF EXISTS lh_bnj_silver.{src_catalog}.silver_contact")
    # spark.sql(f"""CREATE TABLE lh_bnj_silver.{src_catalog}.silver_contact
    # USING DELTA
    # LOCATION '{out_path}'
    # """)


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
    try:
        detail = safe_exception_text(e)
    except Exception:
        detail = traceback.format_exc()

    end_job_instance(batch_id, job_id_str, "FAILED",
                     msg=f"FAILED SILVER src_table={src_table} -> {tgt_table}. {str(e)}"[:8000])

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
