# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Xero Bronze → Silver: `contacts` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **contacts** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260122170500
job_id = 1828              # required for logging (4-digit int/string is OK)
src_catalog = "xero"
job_group_name = "silver"
src_table = "brz_contacts"
tgt_table = "silver_contacts"

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

def transform_xero_contacts(): 
    """
    Transform bronze_xero_contacts to silver_xero_contacts.
    Customers and Suppliers in XERO.
    
    XERO Bronze structure has nested 'records.' prefix for all fields.
    """
    
    bronze = spark.table(src_table)
    
    silver = bronze.select(
        col("`records.contact_id`").alias("contact_id"),
        col("`records.name`").alias("contact_name"),
        
        # Type
        when(col("`records.is_customer`") == True, "Customer")
        .when(col("`records.is_supplier`") == True, "Supplier")
        .otherwise("Other").alias("contact_type"),
        col("`records.is_customer`").cast(BooleanType()).alias("is_customer"),
        col("`records.is_supplier`").cast(BooleanType()).alias("is_supplier"),
        
        # Contact details
        col("`records.email_address`").alias("email"),
        col("`records.first_name`").alias("first_name"),
        col("`records.last_name`").alias("last_name"),
        
        # Status
        col("`records.contact_status`").alias("status"),
        
        # Default currency
        coalesce(col("`records.default_currency`"), lit("SGD")).alias("default_currency"),
        
        # Metadata
        col("tenant_id").alias("tenant_id"),
        col("extracted_at").alias("extracted_at"),
        
        # Timestamps
        col("`records.updated_date_utc`").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["contact_id"], "updated_at")
    
    return silver


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute transformation
try:

    xero_contacts_df = transform_xero_contacts()
    row_cnt = log_data_quality("silver_xero_contacts", xero_contacts_df, "contact_id")
    xero_contacts_df.write.format("delta").mode("overwrite").saveAsTable(tgt_table)

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

mssparkutils.notebook.exit(json.dumps(result_payload, ensure_ascii=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
