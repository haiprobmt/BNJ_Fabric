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

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260122170500
job_id = 1081              # required for logging (4-digit int/string is OK)
src_catalog = "xero"
job_group_name = "silver"
src_table = "brz_accounts"
tgt_table = "silver_accounts"

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

src_table = f"`{WORKSPACE_NAME}`.lh_bnj_bronze.{src_catalog}.{src_table}"
tgt_table = f"`{WORKSPACE_NAME}`.lh_bnj_silver.{src_catalog}.{tgt_table}"
job_id_str = str(job_id).strip()

print(src_table)
print(tgt_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_xero_accounts():
    """
    Transform bronze_xero_accounts to silver_xero_accounts.
    Chart of Accounts.
    
    XERO Bronze structure has nested 'records.' prefix for all fields.
    """
    
    bronze = spark.table(src_table)
    
    silver = bronze.select(
        col("`records.account_id`").alias("account_id"),
        col("`records.code`").alias("account_code"),
        col("`records.name`").alias("account_name"),
        
        # Type and class
        col("`records.type`").alias("account_type"),
        col("`records._class`").alias("account_class"),
        
        # Tax
        col("`records.tax_type`").alias("tax_type"),
        
        # Status
        col("`records.status`").alias("status"),
        coalesce(col("`records.enable_payments_to_account`"), lit(False)).cast(BooleanType()).alias("enable_payments"),
        
        # System account flag
        col("`records.system_account`").alias("system_account_type"),
        
        # Bank account details (if applicable)
        col("`records.bank_account_number`").alias("bank_account_number"),
        col("`records.bank_account_type`").alias("bank_account_type"),
        coalesce(col("`records.currency_code`"), lit("SGD")).alias("currency_code"),
        
        # Metadata
        col("tenant_id").alias("tenant_id"),
        col("extracted_at").alias("extracted_at"),
        
        # Timestamps
        col("`records.updated_date_utc`").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["account_id"], "updated_at")
    
    return silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute transformation
try:
    
    xero_accounts_df = transform_xero_accounts()
    row_cnt = log_data_quality("silver_xero_accounts", xero_accounts_df, "account_id")
    xero_accounts_df.write.format("delta").mode("overwrite").saveAsTable(tgt_table)
    
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

mssparkutils.notebook.exit(json.dumps(result_payload, ensure_ascii=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
