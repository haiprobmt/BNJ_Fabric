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

# # Plato Bronze → Silver: `supplier` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **supplier** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '6858'              # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "supplier"

BRONZE_TABLES = {"plato_supplier": "lh_bnj_bronze.plato.brz_supplier"}
SILVER_TABLES = {"supplier": "lh_bnj_silver.plato.silver_supplier"}
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

def transform_supplier():
    """
    Transform bronze_plato_supplier to silver_supplier.
    
    PLATO Supplier Schema:
    - _id: MongoDB ObjectId (primary key)
    - name: Supplier name
    - category: Supplier category/type
    - email, handphone, telephone, fax
    - address, person (contact person)
    - url, notes, others
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_supplier"])
    
    silver = bronze.select(
        # Primary identifiers
        col("_id").alias("supplier_id"),
        clean_string_column("name").alias("supplier_name"),
        coalesce(clean_string_column("category"), lit("General")).alias("supplier_category"),
        
        # Contact person
        clean_string_column("person").alias("contact_person"),
        
        # Contact details
        clean_string_column("email").alias("email"),
        regexp_replace(col("telephone"), "[^0-9+]", "").alias("phone"),
        regexp_replace(col("handphone"), "[^0-9+]", "").alias("mobile"),
        regexp_replace(col("fax"), "[^0-9+]", "").alias("fax"),
        
        # Address
        clean_string_column("address").alias("address"),
        
        # Additional info
        clean_string_column("url").alias("website"),
        clean_string_column("notes").alias("notes"),
        clean_string_column("others").alias("other_info"),
        
        # Status - default to active (no hidden field in schema)
        lit(True).alias("is_active"),
        
        # Timestamps
        parse_timestamp("created_on").alias("created_at"),
        parse_timestamp("last_edited").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["supplier_id"], "updated_at")
    
    return silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start silver supplier")
try:
    df = transform_supplier()
    src_cnt = spark.table(BRONZE_TABLES["plato_supplier"]).count()
    tgt_cnt = df.count()
    log_data_quality("silver_supplier", df, "supplier_id")
    target_path = f"{lh_silver_path}/silver_supplier"
    df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(target_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES['supplier']} USING DELTA LOCATION '{target_path}'")
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created silver_supplier", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed silver supplier. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
