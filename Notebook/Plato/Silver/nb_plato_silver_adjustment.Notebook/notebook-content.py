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
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         },
# META         {
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
# META         },
# META         {
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Plato Bronze → Silver: `adjustment` (Single Table)


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260204173700
job_id = '6858'              # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "adjustment"

BRONZE_TABLES = {"adjustment": "`WS-ETL-BNJ`.lh_bnj_bronze.plato.brz_adjustment"}
SILVER_TABLES = {"adjustment": "`WS-ETL-BNJ`.lh_bnj_silver.plato.silver_adjustment"}
lh_silver_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/e8a5651c-a1b6-49de-b2c5-11b7e197e77a/Tables/plato'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT count(1) FROM lh_bnj_bronze.plato.brz_adjustment LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This cell is generated from runtime parameters. Learn more: https://go.microsoft.com/fwlink/?linkid=2161015
batch_id = "20260203161300"
job_id = "1383"


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

def transform_silver_adjustment():
    """
    Transform bronze_plato_adjustment to silver_adjustment.
    Inventory adjustments (stock in/out, write-offs, etc.) at LINE LEVEL.
    
    Bronze Schema columns:
    - _id, date, remarks, location, invoice, reason, serial
    - finalized, finalized_on, finalized_by
    - inventory.adjustment_id, inventory.batch, inventory.cpu, inventory.expiry
    - inventory.given_id, inventory.qty, inventory.remarks
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["adjustment"])
    silver = bronze.select(
        # Primary key
        col("_id").alias("adjustment_id"),
        
        # Date
        col("date").alias("adjustment_date"),
        
        # Location and references
        col("location").alias("location_id"),
        col("invoice").alias("invoice_id"),
        col("serial").alias("serial_number"),
        
        # Reason / Type
        col("reason").alias("adjustment_reason"),
        col("remarks").alias("remarks"),
        
        # Inventory line-level info - use backticks for flattened MongoDB fields
        col("`inventory.adjustment_id`").alias("inventory_adjustment_id"),
        col("`inventory.given_id`").alias("item_id"),  # Product/Item identifier
        col("`inventory.batch`").alias("batch_number"),
        col("`inventory.expiry`").cast(LongType()).alias("expiry_date"),  # Epoch timestamp
        col("`inventory.cpu`").cast(DecimalType(18, 4)).alias("cost_per_unit"),
        col("`inventory.qty`").cast(DecimalType(18, 2)).alias("quantity"),
        col("`inventory.remarks`").alias("line_remarks"),
        
        # Finalized status
        when(col("finalized") == 1, True).otherwise(False).alias("is_finalized"),
        col("finalized_on").alias("finalized_at"),
        col("finalized_by").alias("finalized_by"),
        
        # Timestamps
        col("created_on").alias("created_at"),
        col("created_by").alias("created_by"),
        col("last_edited").alias("updated_at"),
        col("last_edited_by").alias("updated_by")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate - use composite key for line-level data
    silver = deduplicate(silver, ["adjustment_id", "inventory_adjustment_id"], "updated_at")
    
    return silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start silver adjustment")
try:
    df = transform_silver_adjustment()
    src_cnt = spark.table(BRONZE_TABLES["adjustment"]).count()
    tgt_cnt = df.count()
    log_data_quality("silver_adjustment", df, "adjustment_id")
    target_path = f"{lh_silver_path}/silver_adjustment"
    df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(target_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES['adjustment']} USING DELTA LOCATION '{target_path}'")
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created silver_adjustment", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed silver adjustment. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
