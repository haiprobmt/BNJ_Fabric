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

# # Plato Bronze → Silver: `inventory` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **inventory** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = "20260207145530"
job_id = "5722"             # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "brz_inventory"

BRONZE_TABLES = {"plato_inventory": "lh_bnj_bronze.plato.brz_inventory"}
SILVER_TABLES = {"inventory": "lh_bnj_silver.plato.silver_inventory"}
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

# MAGIC %%sql 
# MAGIC select count(1) from lh_bnj_silver.plato.silver_inventory

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_inventory():
    """
    Transform bronze_plato_inventory to silver_inventory.
    Key transformations:
    - Standardize product categories
    - Map PLATO column names to silver schema
    
    PLATO Inventory Schema:
    - _id: MongoDB ObjectId (primary key)
    - given_id: Item code
    - name: Item name
    - category, description
    - selling_price, cost_price
    - qty, unit, order_unit
    - dosage, dusage, ddose, dunit, dfreq, dduration (medication fields)
    - supplier, manufacturer, pack_size
    - scheme (e.g., CHAS, MediSave)
    - track_stock, hidden, no_discount, fixed_price
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_inventory"])
    
    silver = bronze.select(
        # Primary identifiers
        col("_id").alias("product_id"),
        col("given_id").alias("product_code"),
        clean_string_column("name").alias("product_name"),
        clean_string_column("description").alias("description"),
        
        # Category standardization
        coalesce(clean_string_column("category"), lit("Uncategorized")).alias("category"),
        
        # Derive product type from category
        when(upper(col("category")).rlike("DRUG|MED|PILL|TAB|CAP|INJECTION|SYRUP"), "Medication")
        .when(upper(col("category")).rlike("CONSUM|DISPOSABLE|GLOVE|MASK|SYRINGE"), "Consumable")
        .when(upper(col("category")).rlike("EQUIP|DEVICE|MACHINE"), "Equipment")
        .when(upper(col("category")).rlike("SERVICE|CONSULT"), "Service")
        .otherwise("Other").alias("product_type"),
        
        # Units
        coalesce(clean_string_column("unit"), lit("EA")).alias("unit_of_measure"),
        clean_string_column("order_unit").alias("order_unit"),
        
        # Pricing
        col("cost_price").cast(DecimalType(18, 4)).alias("unit_cost"),
        col("selling_price").cast(DecimalType(18, 2)).alias("selling_price"),
        
        # Stock management
        col("qty").cast(DecimalType(18, 2)).alias("quantity_on_hand"),
        col("pack_size").cast(IntegerType()).alias("pack_size"),
        
        # Medication-specific fields
        clean_string_column("dosage").alias("dosage"),
        clean_string_column("dusage").alias("dosage_usage"),
        clean_string_column("ddose").alias("default_dose"),
        clean_string_column("dunit").alias("dose_unit"),
        clean_string_column("dfreq").alias("dose_frequency"),
        clean_string_column("dduration").alias("dose_duration"),
        
        # Supplier & Manufacturer
        col("supplier").alias("supplier_id"),
        clean_string_column("manufacturer").alias("manufacturer"),
        
        # Subsidy/scheme info
        clean_string_column("scheme").alias("scheme"),
        
        # Flags - cast BIGINT to Boolean (0=false, non-zero=true)
        coalesce(col("track_stock").cast(BooleanType()), lit(True)).alias("is_track_stock"),
        when(col("hidden").cast(BooleanType()) == True, lit(False)).otherwise(lit(True)).alias("is_active"),
        coalesce(col("no_discount").cast(BooleanType()), lit(False)).alias("no_discount"),
        coalesce(col("fixed_price").cast(BooleanType()), lit(False)).alias("fixed_price"),
        
        # Timestamps
        parse_timestamp("created_on").alias("created_at"),
        parse_timestamp("last_edited").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["product_id"], "updated_at")
    
    return silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start silver inventory")
try:
    df = transform_inventory()
    src_cnt = spark.table(BRONZE_TABLES["plato_inventory"]).count()
    tgt_cnt = df.count()
    log_data_quality("silver_inventory", df, "product_id")
    target_path = f"{lh_silver_path}/silver_inventory"

    df.write.format("delta").mode("overwrite").save(target_path)

    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES['inventory']} USING DELTA LOCATION '{target_path}'")
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created silver_inventory", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed silver inventory. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
