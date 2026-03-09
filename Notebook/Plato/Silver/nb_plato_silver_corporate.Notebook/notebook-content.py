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

# # Plato Bronze → Silver: `corporate` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **corporate** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260204173700
job_id = '6858'              # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "corporate"

BRONZE_TABLES = {"plato_corporate": "lh_bnj_bronze.plato.brz_corporate"}
SILVER_TABLES = {"corporate": "lh_bnj_silver.plato.silver_corporate"}
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

def transform_corporate():
    """
    Transform bronze_plato_corporate to silver_corporate.
    Corporate accounts include insurance companies and corporate clients.
    
    PLATO Corporate Schema:
    - _id: MongoDB ObjectId (primary key)
    - given_id: Corporate code
    - name: Corporate/Insurance name
    - category: Type of corporate (Insurance, Corporate, etc.)
    - email, telephone, fax, address, person
    - url, notes
    - insurance: Flag (1 = insurance company)
    - payment_type, amount, invoice_notes
    - scheme, discounts, specials, restricted, others
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_corporate"])
    
    silver = bronze.select(
        # Primary identifiers
        col("_id").alias("corporate_id"),
        col("given_id").alias("corporate_code"),
        clean_string_column("name").alias("corporate_name"),
        
        # Type classification - use category and insurance flag
        when(col("insurance") == 1, "Insurance")
        .when(upper(col("category")).contains("INSUR"), "Insurance")
        .when(upper(col("category")).contains("CORP"), "Corporate")
        .when(upper(col("category")).contains("GOV"), "Government")
        .otherwise(coalesce(col("category"), lit("Other"))).alias("corporate_type"),
        
        # Category as-is
        col("category").alias("category"),
        
        # Contact
        clean_string_column("person").alias("contact_person"),
        clean_string_column("email").alias("email"),
        regexp_replace(col("telephone"), "[^0-9+]", "").alias("phone"),
        regexp_replace(col("fax"), "[^0-9+]", "").alias("fax"),
        
        # Address
        clean_string_column("address").alias("address"),
        
        # Website
        clean_string_column("url").alias("website"),
        
        # Payment and billing
        clean_string_column("payment_type").alias("payment_type"),
        col("amount").cast(DecimalType(18, 2)).alias("credit_amount"),
        
        # Scheme and discounts
        clean_string_column("scheme").alias("scheme"),
        clean_string_column("discounts").alias("discounts"),
        clean_string_column("specials").alias("specials"),
        clean_string_column("restricted").alias("restricted_items"),
        
        # Notes
        clean_string_column("notes").alias("notes"),
        clean_string_column("invoice_notes").alias("invoice_notes"),
        clean_string_column("others").alias("other_info"),
        
        # Insurance flag
        when(col("insurance") == 1, True).otherwise(False).alias("is_insurance"),
        
        # Status - default to active
        lit(True).alias("is_active"),
        
        # Timestamps
        parse_timestamp("created_on").alias("created_at"),
        parse_timestamp("last_edited").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["corporate_id"], "updated_at")
    
    return silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start silver corporate")
try:
    df = transform_corporate()
    src_cnt = spark.table(BRONZE_TABLES["plato_corporate"]).count()
    tgt_cnt = df.count()
    log_data_quality("silver_corporate", df, "corporate_id")
    target_path = f"{lh_silver_path}/silver_corporate"
    df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(target_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES['corporate']} USING DELTA LOCATION '{target_path}'")
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created silver_corporate", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed silver corporate. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
