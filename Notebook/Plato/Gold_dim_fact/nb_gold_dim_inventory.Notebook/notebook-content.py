# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "102823e0-12f1-4ca5-b61b-a2df5d75beb2",
# META       "default_lakehouse_name": "lh_bnj_gold",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         },
# META         {
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
# META         },
# META         {
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         },
# META         {
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold: dim_inventory


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '6858'
src_catalog = "plato"
job_group_name = "gold"
src_table = "silver_inventory"
tgt_catalog = "gold"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


SILVER_TABLES = {"inventory": f"lh_bnj_silver.{src_catalog}.silver_inventory"}
GOLD_DIMENSIONS = {"inventory": f"lh_bnj_gold.gold.dim_inventory"}

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

def create_dim_inventory():
    """
    Create inventory dimension from silver_inventory table.
    
    Target schema (37 columns):
    - category, cost_price, description, dosage, dosage_dose, dosage_duration
    - dosage_frequency, dosage_unit, dosage_usage, dw_created_at, dw_updated_at
    - effective_date, end_date, given_id, has_expiry_after_dispensing
    - inventory_id, inventory_key, inventory_type, is_current, is_fixed_price
    - is_hidden, is_redeemable, item_name, manufacturer, min_price, no_discount
    - order_unit, pack_size, package_original_price, precautions, redemption_count
    - selling_price, source_system, supplier_id, track_stock, unit
    """
    
    # Read silver inventory data
    silver_inventory = spark.table(SILVER_TABLES["inventory"])
    
    # Transform to dimension with exact schema
    dim_inventory = silver_inventory.select(
        # Surrogate key
        #monotonically_increasing_id().alias("inventory_key"),
        
        # Natural key
        col("product_id").alias("inventory_id"),
        col("product_code").alias("given_id"),
        
        # Descriptive columns
        col("product_name").alias("item_name"),
        col("description"),
        col("category"),
        col("product_type").alias("inventory_type"),
        
        # Pricing
        col("unit_cost").cast(DecimalType(18, 4)).alias("cost_price"),
        col("selling_price").cast(DecimalType(18, 2)),
        lit(None).cast(DecimalType(18, 2)).alias("min_price"),
        lit(None).cast(DecimalType(18, 2)).alias("package_original_price"),
        
        # Units and packaging
        col("unit_of_measure").alias("unit"),
        col("order_unit"),
        col("pack_size"),
        
        # Dosage information
        col("dosage"),
        col("default_dose").alias("dosage_dose"),
        col("dose_unit").alias("dosage_unit"),
        col("dose_frequency").alias("dosage_frequency"),
        col("dose_duration").alias("dosage_duration"),
        col("dosage_usage"),
        lit(None).cast(StringType()).alias("precautions"),
        
        # Supplier and manufacturer
        col("supplier_id"),
        col("manufacturer"),
        
        # Flags
        col("is_track_stock").alias("track_stock"),
        coalesce(col("fixed_price"), lit(False)).alias("is_fixed_price"),
        coalesce(col("no_discount"), lit(False)).alias("no_discount"),
        lit(False).alias("is_hidden"),
        lit(False).alias("is_redeemable"),
        lit(0).alias("redemption_count"),
        lit(False).alias("has_expiry_after_dispensing"),
        
        # Source system
        lit("PLATO").alias("source_system"),
        
        # SCD Type 2 columns
        current_date().alias("effective_date"),
        lit(None).cast(DateType()).alias("end_date"),
        lit(True).alias("is_current"),
        
        # Data warehouse metadata
        current_timestamp().alias("dw_created_at"),
        current_timestamp().alias("dw_updated_at")
    ).dropDuplicates(["inventory_id"])
    
    # Reorder columns alphabetically to match target schema
    result = dim_inventory.select(
        #col("inventory_key"),
        col("inventory_id"),
        col("supplier_id"),
        col("given_id"),
        col("category"),
        col("inventory_type"),
        col("description"),
        col("dosage"),
        col("dosage_dose"),
        col("dosage_duration"),
        col("dosage_frequency"),
        col("dosage_unit"),
        col("has_expiry_after_dispensing"),
        col("is_hidden"),
        col("is_redeemable"),
        col("item_name"),
        col("manufacturer"),
        col("no_discount"),
        col("order_unit"),
        col("pack_size"),
        col("precautions"),
        col("redemption_count"),
        col("source_system"),
        col("track_stock"),
        col("is_fixed_price"),
        col("unit"),
        col("dosage_usage"),
        col("selling_price"),
        col("min_price"),
        col("cost_price"),
        col("package_original_price"),
        col("is_current"),
        col("end_date"),
        col("effective_date"),
        col("dw_created_at"),
        col("dw_updated_at")
    )
    
    return result


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    src_df = create_dim_inventory()
    src_cnt = spark.table(SILVER_TABLES["inventory"]).count()
    tgt_cnt = src_df.count()
    log_data_quality("dim_inventory", src_df, "inventory_id")

    merge_dimension(
        src_df,
        GOLD_DIMENSIONS["inventory"],
        ["inventory_id"],
        "inventory_key"
    )

    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created dim_inventory", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed dim_inventory. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    src_df = create_dim_inventory()
    src_cnt = spark.table(SILVER_TABLES["inventory"]).count()
    tgt_cnt = src_df.count()
    log_data_quality("dim_inventory", src_df, "inventory_key")
    src_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_DIMENSIONS["inventory"])
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created dim_inventory", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed dim_inventory. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
