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

# # Plato Bronze → Silver: `payment` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **payment** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '6858'              # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "payment"

BRONZE_TABLES = {"plato_payment": "lh_bnj_bronze.plato.brz_payment"}
SILVER_TABLES = {"payment": "lh_bnj_silver.plato.silver_payment"}
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

def transform_payment():
    """
    Transform bronze_plato_payment to silver_payment.
    
    Source columns from PLATO payment schema:
    - _id, invoice_id, mode, corp, amount
    - location, ref, session
    - void, void_on, void_by, void_reason
    - created_on, created_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_payment"])
    bronze = standardize_column_names(bronze)
    
    silver = bronze.select(
        # Primary key
        col("_id").alias("payment_id"),
        
        # Relationships
        col("invoice_id").alias("invoice_id"),
        col("corp").alias("corporate_id"),
        clean_string_column("location").alias("location_id"),
        
        # Payment method - PLATO uses 'mode'
        clean_string_column("mode").alias("payment_mode_raw"),
        when(upper(col("mode")).contains("CASH"), "Cash")
        .when(upper(col("mode")).contains("CARD") | 
              upper(col("mode")).contains("CREDIT") |
              upper(col("mode")).contains("VISA") |
              upper(col("mode")).contains("MASTER"), "Credit Card")
        .when(upper(col("mode")).contains("NETS") |
              upper(col("mode")).contains("DEBIT"), "Debit Card")
        .when(upper(col("mode")).contains("PAYNOW") |
              upper(col("mode")).contains("TRANSFER"), "Bank Transfer")
        .when(upper(col("mode")).contains("CHEQUE") |
              upper(col("mode")).contains("CHECK"), "Cheque")
        .when(upper(col("mode")).contains("CORPORATE") |
              upper(col("mode")).contains("CORP"), "Corporate")
        .when(upper(col("mode")).contains("INSURANCE") |
              upper(col("mode")).contains("CLAIM"), "Insurance")
        .otherwise(coalesce(col("mode"), lit("Other"))).alias("payment_method"),
        
        # Amount
        col("amount").cast(DecimalType(18, 4)).alias("payment_amount"),
        
        # Reference
        clean_string_column("ref").alias("reference"),
        col("session").alias("session"),
        
        # Void info
        when(col("void") == 1, True).otherwise(False).alias("is_void"),
        clean_string_column("void_on").alias("voided_at"),
        clean_string_column("void_by").alias("voided_by"),
        clean_string_column("void_reason").alias("void_reason"),
        
        # Timestamps
        parse_timestamp("created_on").alias("created_at"),
        clean_string_column("created_by").alias("created_by")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["payment_id"], "created_at")
    
    return silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    df = transform_payment()
    src_cnt = spark.table(BRONZE_TABLES["plato_payment"]).count()
    tgt_cnt = df.count()
    log_data_quality("silver_payment", df, "payment_id")
    target_path = f"{lh_silver_path}/silver_payment"
    df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(target_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES['payment']} USING DELTA LOCATION '{target_path}'")
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created silver_payment", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed silver payment. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
