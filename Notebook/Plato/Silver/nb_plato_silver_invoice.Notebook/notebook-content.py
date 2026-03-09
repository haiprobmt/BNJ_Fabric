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

# # Plato Bronze → Silver: `invoice` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **invoice** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '6858'              # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "invoice"

BRONZE_TABLES = {"plato_invoice": "lh_bnj_bronze.plato.brz_invoice"}
SILVER_TABLES = {"invoice": "lh_bnj_silver.plato.silver_invoice"}
lh_silver_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/e8a5651c-a1b6-49de-b2c5-11b7e197e77a/Tables/plato'


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

def transform_invoice():
    """
    Transform bronze_plato_invoice to silver_invoice.
    
    PLATO Schema columns:
    - _id, patient_id, location, date, doctor, status, status_on
    - sub_total, tax, total, adj_amount, adj
    - invoice (number), invoice_prefix, finalized, void
    - corporate, payment, item, mc, prescription (may be flattened)
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_invoice"])
    
    silver = bronze.select(
        # Primary key
        optional_col(bronze, "_id", "invoice_id"),
        
        # Invoice number (combine prefix + number)
        concat(
            coalesce(optional_col(bronze, "invoice_prefix"), lit("")),
            coalesce(optional_col(bronze, "invoice").cast("string"), lit(""))
        ).alias("invoice_number"),
        
        # References
        optional_col(bronze, "patient_id"),
        optional_col(bronze, "doctor", "doctor_id"),
        optional_col(bronze, "location", "location_id"),
        optional_col(bronze, "corporate._id", "corporate_id"),
        
        # Date
        optional_col(bronze, "date", "invoice_date"),
        
        # Amounts
        optional_col(bronze, "sub_total", "subtotal", DecimalType(18, 4)),
        optional_col(bronze, "tax", "tax_amount", DecimalType(18, 4)),
        optional_col(bronze, "total", "total_amount", DecimalType(18, 4)),
        coalesce(
            optional_col(bronze, "adj_amount"),
            lit(0)
        ).cast(DecimalType(18, 4)).alias("adjustment_amount"),
        
        # GST flag
        when(optional_col(bronze, "no_gst") == 1, False).otherwise(True).alias("gst_applicable"),
        optional_col(bronze, "rate", "tax_rate"),
        
        # Status flags
        optional_col(bronze, "status"),
        optional_col(bronze, "status_on", "status_changed_at"),
        when(optional_col(bronze, "finalized") == 1, True).otherwise(False).alias("is_finalized"),
        optional_col(bronze, "finalized_on", "finalized_at"),
        optional_col(bronze, "finalized_by"),
        
        # Void info
        when(optional_col(bronze, "void") == 1, True).otherwise(False).alias("is_void"),
        optional_col(bronze, "void_reason"),
        optional_col(bronze, "void_on", "voided_at"),
        optional_col(bronze, "void_by", "voided_by"),
        
        # Credit/Debit note
        when(optional_col(bronze, "cndn") == 1, True).otherwise(False).alias("is_credit_debit_note"),
        optional_col(bronze, "cndn_apply_to", "cndn_applied_to_invoice"),
        
        # Additional info
        optional_col(bronze, "scheme"),
        optional_col(bronze, "session", "session_number"),
        optional_col(bronze, "highlight", "is_highlighted", BooleanType()),
        
        # Notes
        optional_col(bronze, "notes"),
        optional_col(bronze, "corp_notes", "corporate_notes"),
        optional_col(bronze, "invoice_notes"),
        
        # Time tracking
        optional_col(bronze, "manual_timein", "manual_time_in"),
        optional_col(bronze, "manual_timeout", "manual_time_out"),
        
        # Timestamps
        optional_col(bronze, "created_on", "created_at"),
        optional_col(bronze, "created_by"),
        optional_col(bronze, "last_edited", "updated_at"),
        optional_col(bronze, "last_edited_by", "updated_by")
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

#start_job_instance(batch_id, job_id, msg="Start silver invoice")
try:
    df = transform_invoice()
    src_cnt = spark.table(BRONZE_TABLES["plato_invoice"]).count()
    tgt_cnt = df.count()
    log_data_quality("silver_invoice", df, "invoice_id")
    target_path = f"{lh_silver_path}/silver_invoice"
    df.write.format("delta").mode("overwrite").save(target_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES['invoice']} USING DELTA LOCATION '{target_path}'")
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created silver_invoice", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed silver invoice. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- helper: enforce schema from existing silver table using try_cast (Option 1) ---
def enforce_to_table_schema_try_cast(df, target_table: str):
    target_schema = spark.table(target_table).schema
    exprs = []
    for f in target_schema.fields:
        name = f.name
        sql_type = f.dataType.simpleString().lower()
        if name in df.columns:
            exprs.append(F.expr(f"try_cast(`{name}` as {sql_type})").alias(name))
        else:
            exprs.append(F.expr(f"cast(null as {sql_type})").alias(name))
    return df.select(*exprs)

#start_job_instance(batch_id, job_id, msg="Start silver invoice")
try:
    src_cnt = spark.table(BRONZE_TABLES["plato_invoice"]).count()
    target_path = f"{lh_silver_path}/silver_invoice"
    silver_table = SILVER_TABLES["invoice"]

    # 1) build transformed df (your existing logic)
    df_raw = transform_invoice()

    # 2) Make overwrite tolerant to schema drift:
    #    - If table exists -> enforce to its schema (try_cast)
    #    - If table does not exist -> write once with overwriteSchema
    if spark.catalog.tableExists(silver_table):
        df = enforce_to_table_schema_try_cast(df_raw, silver_table)
        # keep row count after enforcement (may change only in types -> NULL, row count same)
        tgt_cnt = df.count()

        log_data_quality("silver_invoice", df, "invoice_id")

        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "false") \
            .save(target_path)

    else:
        # First time: create table & schema from df_raw
        tgt_cnt = df_raw.count()
        log_data_quality("silver_invoice", df_raw, "invoice_id")

        df_raw.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(target_path)

        spark.sql(f"CREATE TABLE IF NOT EXISTS {silver_table} USING DELTA LOCATION '{target_path}'")

    # Ensure table points to the path (safe to run)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {silver_table} USING DELTA LOCATION '{target_path}'")

    end_job_instance(
        batch_id, job_id, "SUCCESS",
        msg="Created silver_invoice (overwrite, schema-tolerant)",
        src_row_num=src_cnt, tgt_row_num=tgt_cnt
    )
    print("OK")

except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed silver invoice. {safe_exception_text(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
