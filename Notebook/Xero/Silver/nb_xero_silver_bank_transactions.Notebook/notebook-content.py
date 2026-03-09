# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Xero Bronze → Silver: `bank_transactions` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **bank_transactions** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260204173700
job_id = 6771             # required for logging (4-digit int/string is OK)
src_catalog = "xero"
job_group_name = "silver"
src_table = "brz_bank_transactions"
tgt_table = "silver_bank_transactions"

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

def transform_xero_bank_transactions():
    """
    Transform bronze_xero_bank_transactions to silver_xero_bank_transactions.
    
    XERO Bronze structure has nested 'records.' prefix for all fields.
    """
    
    bronze = spark.table(src_table)
    
    silver = bronze.select(
        col("`records.bank_transaction_id`").alias("bank_transaction_id"),
        col("`records.bank_account.account_id`").alias("bank_account_id"),
        col("`records.contact.contact_id`").alias("contact_id"),
        
        # Type (RECEIVE, SPEND, etc.)
        col("`records.type`").alias("type"),
        when(col("`records.type`").isin("RECEIVE", "RECEIVE-OVERPAYMENT", "RECEIVE-PREPAYMENT"), "Inflow")
        .when(col("`records.type`").isin("SPEND", "SPEND-OVERPAYMENT", "SPEND-PREPAYMENT"), "Outflow")
        .otherwise("Other").alias("flow_direction"),
        
        # Date
        col("`records.date`").alias("transaction_date"),
        
        # Amounts
        col("`records.sub_total`").cast(DecimalType(18, 2)).alias("subtotal"),
        col("`records.total_tax`").cast(DecimalType(18, 2)).alias("total_tax"),
        col("`records.total`").cast(DecimalType(18, 2)).alias("total"),
        
        # Currency
        coalesce(col("`records.currency_code`"), lit("SGD")).alias("currency_code"),
        
        # Status
        col("`records.status`").alias("status"),
        col("`records.is_reconciled`").cast(BooleanType()).alias("is_reconciled"),
        
        # Reference
        col("`records.reference`").alias("reference"),
        
        # Metadata
        col("tenant_id").alias("tenant_id"),
        col("extracted_at").alias("extracted_at"),
        
        # Timestamps
        col("`records.updated_date_utc`").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["bank_transaction_id"], "updated_at")
    
    return silver


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute transformation
try:
    #start_job_instance(batch_id, job_id_str, msg=f"Start SILVER src_table={src_table} -> {tgt_table}")

    xero_bank_df = transform_xero_bank_transactions()
    row_cnt = log_data_quality("silver_xero_bank_transactions", xero_bank_df, "bank_transaction_id")
    xero_bank_df.write.format("delta").mode("overwrite").saveAsTable(tgt_table)
    
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
