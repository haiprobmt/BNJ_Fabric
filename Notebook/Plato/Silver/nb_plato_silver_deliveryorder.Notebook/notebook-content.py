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

# # Plato Bronze → Silver: `deliveryorder` (Single Table)


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260204173700
job_id = '6858'              # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "deliveryorder"

BRONZE_TABLES = {"plato_deliveryorder": "`WS-ETL-BNJ`.lh_bnj_bronze.plato.brz_deliveryorder"}
SILVER_TABLES = {"deliveryorder": "`WS-ETL-BNJ`.lh_bnj_silver.plato.silver_deliveryorder"}
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

def transform_silver_deliveryorder():
    """
    Transform bronze_plato_deliveryorder to silver_deliveryorder.
    Purchase orders and goods received at LINE LEVEL.
    
    Bronze Schema columns:
    - _id, date, do, serial, invoice, location, supplier
    - sub_total, total, discount, no_gst
    - paid, mode, payment_ref
    - po, remarks, attachments
    - finalized, finalized_on, finalized_by
    - inventory.do_id, inventory.given_id, inventory.batch, inventory.expiry
    - inventory.amount, inventory.pack_size, inventory.qty_ordered
    - inventory.qty_receiving, inventory.qty_bonus, inventory.sample
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_deliveryorder"])
    
    silver = bronze.select(
        # Primary key
        col("_id").alias("delivery_order_id"),
        col("do").alias("do_number"),
        col("serial").alias("serial_number"),
        
        # References
        col("supplier").alias("supplier_id"),
        col("location").alias("location_id"),
        col("invoice").alias("invoice_id"),
        col("po").alias("purchase_order_ref"),
        
        # Date
        col("date").alias("delivery_date"),
        
        # Header-level amounts
        col("sub_total").cast(DecimalType(18, 4)).alias("subtotal"),
        col("total").cast(DecimalType(18, 4)).alias("total_amount"),
        coalesce(col("discount"), lit(0)).cast(DecimalType(18, 4)).alias("discount_amount"),
        
        # GST flag
        when(col("no_gst") == 1, False).otherwise(True).alias("gst_applicable"),
        
        # Payment info
        when(col("paid") == 1, True).otherwise(False).alias("is_paid"),
        col("mode").alias("payment_mode"),
        col("payment_ref").alias("payment_reference"),
        
        # Inventory LINE-LEVEL info - use backticks for flattened MongoDB fields
        col("`inventory.do_id`").alias("inventory_do_id"),
        col("`inventory.given_id`").alias("item_id"),  # Product/Item identifier
        col("`inventory.batch`").alias("batch_number"),
        col("`inventory.expiry`").cast(LongType()).alias("expiry_date"),  # Epoch timestamp
        col("`inventory.amount`").cast(DecimalType(18, 4)).alias("line_amount"),
        col("`inventory.pack_size`").cast(IntegerType()).alias("pack_size"),
        col("`inventory.qty_ordered`").cast(DecimalType(18, 2)).alias("qty_ordered"),
        col("`inventory.qty_receiving`").cast(DecimalType(18, 2)).alias("qty_receiving"),
        col("`inventory.qty_bonus`").cast(DecimalType(18, 2)).alias("qty_bonus"),
        when(col("`inventory.sample`") == 1, True).otherwise(False).alias("is_sample"),
        
        # Additional info
        clean_string_column("remarks").alias("remarks"),
        col("attachments").alias("attachments"),
        
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
    silver = deduplicate(silver, ["delivery_order_id", "inventory_do_id"], "updated_at")
    
    return silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start silver deliveryorder")
try:
    df = transform_silver_deliveryorder()
    src_cnt = spark.table(BRONZE_TABLES["plato_deliveryorder"]).count()
    tgt_cnt = df.count()
    log_data_quality("silver_deliveryorder", df, "delivery_order_id")
    target_path = f"{lh_silver_path}/silver_deliveryorder"
    df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(target_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES['deliveryorder']} USING DELTA LOCATION '{target_path}'")
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created silver_deliveryorder", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed silver deliveryorder. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # LEGACY LOGIC

# CELL ********************

def transform_deliveryorder():
    """
    Transform bronze_plato_deliveryorder to silver_deliveryorder.
    Purchase orders and goods received.
    
    PLATO Schema columns:
    - _id, date, do, serial, invoice, location, supplier
    - sub_total, total, discount, no_gst
    - paid, mode, payment_ref
    - po, remarks, attachments
    - finalized, finalized_on, finalized_by
    - inventory.batch, inventory.do_id (flattened from MongoDB)
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_deliveryorder"])
    
    silver = bronze.select(
        # Primary key
        col("_id").alias("delivery_order_id"),
        col("do").alias("do_number"),
        col("serial").alias("serial_number"),
        
        # References
        col("supplier").alias("supplier_id"),
        col("location").alias("location_id"),
        col("invoice").alias("invoice_id"),
        col("po").alias("purchase_order_ref"),
        
        # Date
        col("date").alias("delivery_date"),
        
        # Amounts
        col("sub_total").cast(DecimalType(18, 4)).alias("subtotal"),
        col("total").cast(DecimalType(18, 4)).alias("total_amount"),
        coalesce(col("discount"), lit(0)).cast(DecimalType(18, 4)).alias("discount_amount"),
        
        # GST flag
        when(col("no_gst") == 1, False).otherwise(True).alias("gst_applicable"),
        
        # Payment info
        when(col("paid") == 1, True).otherwise(False).alias("is_paid"),
        col("mode").alias("payment_mode"),
        col("payment_ref").alias("payment_reference"),
        
        # Inventory info - use backticks for flattened MongoDB fields
        col("`inventory.batch`").alias("inventory_batch"),
        col("`inventory.do_id`").alias("inventory_do_id"),
        
        # Additional info
        clean_string_column("remarks").alias("remarks"),
        col("attachments").alias("attachments"),
        
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
    
    # Deduplicate
    silver = deduplicate(silver, ["delivery_order_id"], "updated_at")
    
    return silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
