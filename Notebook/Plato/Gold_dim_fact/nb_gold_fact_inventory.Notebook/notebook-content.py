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
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         },
# META         {
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         },
# META         {
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Plato Silver → Gold Fact: `fact_inventory`

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260226090900
job_id = '6858'
src_catalog = "plato"
job_group_name = "gold"
src_table = "silver_adjustment"
tgt_catalog = "gold"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


SILVER_TABLES = {"adjustment": f"`WS-ETL-BNJ`.lh_bnj_silver.{src_catalog}.silver_adjustment", "deliveryorder": f"lh_bnj_silver.{src_catalog}.silver_deliveryorder"}
GOLD_FACTS = {"inventory": f"`WS-ETL-BNJ`.lh_bnj_gold.{tgt_catalog}.fact_inventory"}
GOLD_DIMENSIONS = {"location": f"`WS-ETL-BNJ`.lh_bnj_gold.{tgt_catalog}.dim_location", "supplier": f"lh_bnj_gold.{tgt_catalog}.dim_supplier"}

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

def create_fact_inventory():
    """
    Create fact_inventory for Inventory Health score.
    Combines adjustments and delivery orders at LINE LEVEL.
    Grain: One row per inventory line item transaction.
    
    Uses QUANTITY + VALUE metrics:
    - qty_in/qty_out from delivery orders and adjustments
    - value_in/value_out calculated from quantity * cost
    - expiry_date for expired stock tracking
    """
    
    # Read silver data
    silver_adjustment = spark.table(SILVER_TABLES["adjustment"])
    silver_deliveryorder = spark.table(SILVER_TABLES["deliveryorder"])
    
    # Read dimension tables
    dim_location = spark.table(GOLD_DIMENSIONS["location"]).select("location_key", "location_id")
    dim_supplier = spark.table(GOLD_DIMENSIONS["supplier"]).select("supplier_key", "supplier_id")
    
    # Process adjustments (stock adjustments - in/out) - LINE LEVEL
    # Positive quantity = stock in, Negative quantity = stock out
    adjustments = silver_adjustment.select(
        col("adjustment_id").alias("transaction_id"),
        col("inventory_adjustment_id").alias("line_id"),
        col("item_id"),
        col("batch_number"),
        get_date_key(col("adjustment_date")).alias("date_key"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        lit(None).cast(StringType()).alias("supplier_id"),
        
        # Transaction type from reason
        coalesce(col("adjustment_reason"), lit("Adjustment")).alias("transaction_type"),
        
        # Quantity in/out based on sign
        when(col("quantity") > 0, col("quantity")).otherwise(lit(0)).cast(DecimalType(18, 2)).alias("qty_in"),
        when(col("quantity") < 0, abs(col("quantity"))).otherwise(lit(0)).cast(DecimalType(18, 2)).alias("qty_out"),
        coalesce(col("quantity"), lit(0)).cast(DecimalType(18, 2)).alias("quantity"),
        
        # Value in/out based on quantity * cost
        when(col("quantity") > 0, 
             col("quantity") * coalesce(col("cost_per_unit"), lit(0))
        ).otherwise(lit(0)).cast(DecimalType(18, 2)).alias("value_in"),
        when(col("quantity") < 0, 
             abs(col("quantity") * coalesce(col("cost_per_unit"), lit(0)))
        ).otherwise(lit(0)).cast(DecimalType(18, 2)).alias("value_out"),
        
        coalesce(col("cost_per_unit"), lit(0)).cast(DecimalType(18, 4)).alias("unit_cost"),
        
        # Expiry date (epoch to date)
        when(col("expiry_date").isNotNull(), 
             from_unixtime(col("expiry_date") / 1000).cast(DateType())
        ).alias("expiry_date"),
        
        # Reference
        col("remarks"),
        col("invoice_id"),
        col("is_finalized"),
        col("created_at").cast(TimestampType())
    )
    
    # Process delivery orders (purchases - goods received = qty IN) - LINE LEVEL
    deliveries = silver_deliveryorder.select(
        col("delivery_order_id").alias("transaction_id"),
        col("inventory_do_id").alias("line_id"),
        col("item_id"),
        col("batch_number"),
        get_date_key(col("delivery_date")).alias("date_key"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        col("supplier_id"),
        
        lit("Purchase").alias("transaction_type"),
        
        # Quantity received = qty in (include bonus)
        (coalesce(col("qty_receiving"), lit(0)) + coalesce(col("qty_bonus"), lit(0))).cast(DecimalType(18, 2)).alias("qty_in"),
        lit(0).cast(DecimalType(18, 2)).alias("qty_out"),
        (coalesce(col("qty_receiving"), lit(0)) + coalesce(col("qty_bonus"), lit(0))).cast(DecimalType(18, 2)).alias("quantity"),
        
        # Value from line_amount
        coalesce(col("line_amount"), lit(0)).cast(DecimalType(18, 2)).alias("value_in"),
        lit(0).cast(DecimalType(18, 2)).alias("value_out"),
        
        # Calculate unit cost from line_amount / qty_receiving
        when(col("qty_receiving") > 0, 
             col("line_amount") / col("qty_receiving")
        ).otherwise(lit(0)).cast(DecimalType(18, 4)).alias("unit_cost"),
        
        # Expiry date (epoch to date)
        when(col("expiry_date").isNotNull(), 
             from_unixtime(col("expiry_date") / 1000).cast(DateType())
        ).alias("expiry_date"),
        
        # Reference
        col("remarks"),
        col("invoice_id"),
        col("is_finalized"),
        col("created_at").cast(TimestampType())
    )
    
    # Union all inventory movements
    fact_inventory = adjustments.unionByName(deliveries)
    
    # Add surrogate key
    # fact_inventory = fact_inventory.withColumn(
    #     "inventory_fact_key", 
    #     monotonically_increasing_id()
    # )
    
    # Join with dimensions
    fact_inventory = fact_inventory \
        .join(dim_location, "location_id", "left") \
        .join(dim_supplier, "supplier_id", "left")
    
    # Select final columns
    fact_inventory = fact_inventory.select(
        # "inventory_fact_key",
        "transaction_id",
        "invoice_id",
        "line_id",
        "item_id",
        "batch_number",
        "date_key",
        coalesce(col("location_key"), lit(1)).alias("location_key"),
        coalesce(col("supplier_key"), lit(-1)).alias("supplier_key"),
        "transaction_type",
        "qty_in",
        "qty_out",
        "quantity",
        "value_in",
        "value_out",
        "unit_cost",
        "expiry_date",
        "remarks",
        "is_finalized",
        "created_at"
    ).withColumn("is_current", lit(True)) \
    .withColumn("end_date", lit(None).cast(DateType())) \
    .withColumn("effective_date", current_date()) \
    .withColumn("dw_created_at", current_timestamp()) \
    .withColumn("dw_updated_at", current_timestamp())
    
    return fact_inventory

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start fact_inventory")
try:
    src_df = create_fact_inventory()
    src_cnt = spark.table(SILVER_TABLES["adjustment"]).count()
    tgt_table = GOLD_FACTS["inventory"]
    tgt_cnt = src_df.count()
    # log_data_quality("fact_inventory", df, "inventory_key")
    merge_dimension(
        src_df,
        tgt_table,
        ["transaction_id"],
        "inventory_fact_key"
    )  
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created fact_inventory", src_row_num=src_cnt, tgt_row_num=tgt_cnt)

    print(f"✅ Created {tgt_table} with {src_cnt} rows")
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed fact_inventory. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # LECACY LOGIC

# CELL ********************

def create_fact_inventory():
    """
    Create fact_inventory for Inventory Health score.
    Combines adjustments and delivery orders (header level only).
    Grain: One row per inventory transaction.
    
    silver_adjustment columns:
    - adjustment_id, adjustment_date, location_id, invoice_id, serial_number
    - adjustment_reason, remarks, cost_per_unit, quantity
    - is_finalized, finalized_at, created_at, updated_at
    
    silver_deliveryorder columns:
    - delivery_order_id, do_number, serial_number, supplier_id, location_id
    - delivery_date, subtotal, total_amount, discount_amount
    - inventory_batch, inventory_do_id
    - is_paid, is_finalized, created_at, updated_at
    
    Note: PLATO data is header-level only - no line item details like item_id, 
    batch_number per item, or expiry_date. Inventory tracking is at document level.
    """
    
    # Read silver data
    silver_adjustment = spark.table(SILVER_TABLES["adjustment"])
    silver_deliveryorder = spark.table(SILVER_TABLES["deliveryorder"])
    
    # Read dimension tables
    dim_location = spark.table(GOLD_DIMENSIONS["location"]).select("location_key", "location_id")
    dim_supplier = spark.table(GOLD_DIMENSIONS["supplier"]).select("supplier_key", "supplier_id")
    
    # Process adjustments (stock adjustments - in/out)
    adjustments = silver_adjustment.select(
        col("adjustment_id").alias("transaction_id"),
        get_date_key(col("adjustment_date")).alias("date_key"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        lit(None).cast(StringType()).alias("supplier_id"),
        
        # Transaction type from reason
        coalesce(col("adjustment_reason"), lit("Adjustment")).alias("transaction_type"),
        
        # Quantities - positive = in, negative = out
        when(col("quantity") > 0, col("quantity")).otherwise(lit(0)).cast(DecimalType(18, 2)).alias("quantity_in"),
        when(col("quantity") < 0, abs(col("quantity"))).otherwise(lit(0)).cast(DecimalType(18, 2)).alias("quantity_out"),
        
        # Cost
        coalesce(col("cost_per_unit"), lit(0)).cast(DecimalType(18, 2)).alias("unit_cost"),
        (abs(coalesce(col("quantity"), lit(0))) * coalesce(col("cost_per_unit"), lit(0))).cast(DecimalType(18, 2)).alias("total_value"),
        
        # Reference
        col("remarks"),
        col("invoice_id"),
        col("is_finalized"),
        
        # Timestamps
        col("created_at").cast(TimestampType())
    )
    
    # Process delivery orders (purchases - goods received)
    deliveries = silver_deliveryorder.select(
        col("delivery_order_id").alias("transaction_id"),
        get_date_key(col("delivery_date")).alias("date_key"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        col("supplier_id"),
        
        lit("Purchase").alias("transaction_type"),
        
        # For delivery orders, we don't have quantity - use total_amount as proxy
        lit(0).cast(DecimalType(18, 2)).alias("quantity_in"),
        lit(0).cast(DecimalType(18, 2)).alias("quantity_out"),
        
        # Cost - use total_amount at header level
        lit(0).cast(DecimalType(18, 2)).alias("unit_cost"),
        coalesce(col("total_amount"), lit(0)).cast(DecimalType(18, 2)).alias("total_value"),
        
        # Reference
        col("remarks"),
        col("invoice_id"),
        col("is_finalized"),
        
        # Timestamps
        col("created_at").cast(TimestampType())
    )
    
    # Union all inventory movements
    fact_inventory = adjustments.unionByName(deliveries)
    
    # Add surrogate key
    fact_inventory = fact_inventory.withColumn(
        "inventory_fact_key", 
        monotonically_increasing_id()
    )
    
    # Join with dimensions
    fact_inventory = fact_inventory \
        .join(dim_location, "location_id", "left") \
        .join(dim_supplier, "supplier_id", "left")
    
    # Select final columns
    fact_inventory = fact_inventory.select(
        "inventory_fact_key",
        "transaction_id",
        "date_key",
        coalesce(col("location_key"), lit(1)).alias("location_key"),
        coalesce(col("supplier_key"), lit(-1)).alias("supplier_key"),
        "transaction_type",
        "quantity_in",
        "quantity_out",
        "unit_cost",
        "total_value",
        "remarks",
        "invoice_id",
        "is_finalized",
        "created_at"
    )
    
    return fact_inventory

# Create and save fact_inventory
# try:
#     fact_inventory_df = create_fact_inventory()
#     fact_inventory_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACTS["inventory"])
#     print(f"✅ Created {GOLD_FACTS['inventory']} with {fact_inventory_df.count()} rows")
# except Exception as e:
#     print(f"⚠️ Could not create fact_inventory: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
