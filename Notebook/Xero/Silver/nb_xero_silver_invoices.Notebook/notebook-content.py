# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Xero Bronze → Silver: `invoices` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **invoices** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260204173700
job_id = 4542              # required for logging (4-digit int/string is OK)
src_catalog = "xero"
job_group_name = "silver"
src_table = "brz_invoices"
tgt_table = "silver_invoices"

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

def transform_xero_invoices():
    """
    Transform bronze_xero_invoices to silver_xero_invoices.
    ACCREC = Accounts Receivable (sales)
    ACCPAY = Accounts Payable (bills)
    
    XERO Bronze structure has nested 'records.' prefix for all fields.
    e.g., records.invoice_id, records.type, records.contact.contact_id
    """
    
    bronze = spark.table(src_table)
    
    silver = bronze.select(
        # Use backticks for nested column names with dots
        col("`records.invoice_id`").alias("invoice_id"),
        col("`records.invoice_number`").alias("invoice_number"),
        col("`records.contact.contact_id`").alias("contact_id"),
        col("`records.contact.name`").alias("contact_name"),
        
        # Type
        col("`records.type`").alias("type"),  # ACCREC or ACCPAY
        when(col("`records.type`") == "ACCREC", "Sales Invoice")
        .when(col("`records.type`") == "ACCPAY", "Bill")
        .otherwise("Other").alias("invoice_type_desc"),
        
        # Dates
        col("`records.date`").alias("invoice_date"),
        col("`records.due_date`").alias("due_date"),
        
        # Amounts
        col("`records.sub_total`").cast(DecimalType(18, 2)).alias("subtotal"),
        col("`records.total_tax`").cast(DecimalType(18, 2)).alias("total_tax"),
        col("`records.total`").cast(DecimalType(18, 2)).alias("total"),
        col("`records.amount_due`").cast(DecimalType(18, 2)).alias("amount_due"),
        col("`records.amount_paid`").cast(DecimalType(18, 2)).alias("amount_paid"),
        col("`records.amount_credited`").cast(DecimalType(18, 2)).alias("amount_credited"),
        
        # Currency
        coalesce(col("`records.currency_code`"), lit("SGD")).alias("currency_code"),
        coalesce(col("`records.currency_rate`"), lit(1.0)).cast(DecimalType(18, 6)).alias("currency_rate"),
        
        # Status
        col("`records.status`").alias("status"),
        
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
    silver = deduplicate(silver, ["invoice_id"], "updated_at")
    
    return silver


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute transformation
try:

    xero_invoices_df = transform_xero_invoices()
    row_cnt = log_data_quality("silver_xero_invoices", xero_invoices_df, "invoice_id")
    xero_invoices_df.write.format("delta").mode("overwrite").saveAsTable(tgt_table)

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
