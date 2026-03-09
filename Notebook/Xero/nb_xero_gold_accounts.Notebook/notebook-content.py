# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Xero Bronze → Silver: `accounts` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **accounts** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260126095536
job_id = 2681              # required for logging (4-digit int/string is OK)
src_catalog = "xero"
job_group_name = "silver"
src_table = "silver_accounts"
tgt_table = "silver_payments"

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

#==TABLE NAME defined==

src_table = f"`WS-ETL-BNJ`.lh_bnj_silver.{src_catalog}.{src_table}"
tgt_table = f"`WS-ETL-BNJ`.lh_bnj_gold.{src_catalog}.{tgt_table}"
job_id_str = str(job_id).strip()

print(src_table)
print(tgt_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dim_account():
    """
    Create account dimension from XERO accounts.
    """
    
    # Read XERO accounts data
    xero_accounts = spark.table(src_table)
    
    # Transform to dimension
    dim_account = xero_accounts.select(
        monotonically_increasing_id().alias("account_key"),
        col("account_id"),
        col("account_code"),
        col("account_name"),
        col("account_type"),
        col("account_class"),
        col("tax_type"),
        col("system_account_type").alias("is_system_account"),
        col("status").alias("is_active")
    ).dropDuplicates(["account_id"])
    
    return dim_account

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create and save dim_account
try:
    dim_account_df = create_dim_account()
    row_cnt = dim_account_df.count()
    dim_account_df.write.format("delta").mode("overwrite").saveAsTable(tgt_table)
    print(f"✅ Created {tgt_table} with {row_cnt} rows")

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
    
    result_payload = {
        "return_code": -1,
        "return_msg": "FAILED",
        "batch_id": batch_id,
        "job_id": job_id_str,
        "src_table": src_table,
        "tgt_table": tgt_table,
        "error": detail[:8000]
    }
    end_job_instance(batch_id, job_id_str, "FAILED",
                     msg=f"FAILED SILVER src_table={src_table} -> {tgt_table}. {str(e)}"[:8000])

    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
