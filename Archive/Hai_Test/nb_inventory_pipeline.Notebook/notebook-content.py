# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c9d7507e-938a-4c6d-a042-d8743e386ab5",
# META       "default_lakehouse_name": "lh_bnj_bronze",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
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

# # Inventory Pipeline: Bronze → Silver → Gold Fact → Gold Agg
# 
# Complete inventory data pipeline for CFO Dashboard Inventory Health metrics.
# 
# **Data Flow:**
# 1. `brz_adjustment` → `silver_adjustment` (line-level inventory adjustments)
# 2. `brz_deliveryorder` → `silver_deliveryorder` (line-level purchase receipts)
# 3. `silver_adjustment` + `silver_deliveryorder` → `fact_inventory` (unified inventory transactions)
# 4. `fact_inventory` → `agg_inventory` (inventory health metrics)

# CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '9001'
src_catalog = "plato"
job_group_name = "inventory_pipeline"

# Bronze tables
# BRONZE_TABLES = {
#     "adjustment": "lh_bnj_silver.plato.brz_adjustment",
#     "deliveryorder": "lh_bnj_silver.plato.brz_deliveryorder"
# }

# BRONZE_TABLES = {
#     "adjustment": "lh_bnj_silver.plato.brz_adjustment",
#     "deliveryorder": "lh_bnj_silver.plato.brz_deliveryorder"
# }

# Silver tables
SILVER_TABLES = {
    "adjustment": "lh_bnj_silver.plato_test.silver_adjustment",
    "deliveryorder": "lh_bnj_silver.plato_test.silver_deliveryorder"
}
lh_silver_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/e8a5651c-a1b6-49de-b2c5-11b7e197e77a/Tables/plato_test'

# Gold tables
GOLD_FACTS = {"inventory": "lh_bnj_gold.gold_test.fact_inventory"}
GOLD_DIMENSIONS = {
    "location": "lh_bnj_gold.gold_test.dim_location",
    "supplier": "lh_bnj_gold.gold_test.dim_supplier",
    "date": "lh_bnj_gold.gold_test.dim_date"
}
AGG_TABLES = {"inventory": "lh_bnj_gold.gold_test.agg_inventory"}

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

# MARKDOWN ********************

# ## Step 1: Bronze → Silver Adjustment
# 
# Transform `brz_adjustment` to `silver_adjustment` with line-level inventory data.

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

# MARKDOWN ********************

# ## Step 2: Bronze → Silver Delivery Order
# 
# Transform `brz_deliveryorder` to `silver_deliveryorder` with line-level inventory data.

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
    
    bronze = spark.table(BRONZE_TABLES["deliveryorder"])
    
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

display(transform_silver_deliveryorder())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Silver → Gold Fact Inventory
# 
# Combine `silver_adjustment` and `silver_deliveryorder` into unified `fact_inventory`.

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
        "invoice_id",
        "is_finalized",
        "created_at"
    )
    
    return fact_inventory

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(create_fact_inventory())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Gold Fact → Gold Agg Inventory
# 
# Create `agg_inventory` with inventory health metrics for CFO Dashboard.

# CELL ********************

def create_agg_inventory():
    """
    Create agg_inventory aggregate table for Inventory Health dashboard.
    Tracks turnover ratio, expired stock, and DIO metrics.
    
    Key metrics:
    - qty_in/qty_out: Quantity movements
    - value_in/value_out: Value movements
    - expiry_date: For expired stock calculation
    - Inventory turnover ratio
    - Days Inventory Outstanding (DIO)
    - Inventory Health Score
    """
    
    # Read fact_inventory
    fact_inventory = spark.table(GOLD_FACTS["inventory"])
    
    # Get current date for expiry calculation
    current_date_val = current_date()
    
    # Calculate expired stock metrics first (before aggregation)
    fact_with_expiry = fact_inventory.withColumn(
        "is_expired",
        when(col("expiry_date").isNotNull() & (col("expiry_date") < current_date_val), True).otherwise(False)
    )
    
    # Aggregate inventory movements by date
    inv_agg = fact_with_expiry.groupBy("date_key") \
        .agg(
            # Quantity flows
            sum("qty_in").cast(DecimalType(18,2)).alias("total_qty_in"),
            sum("qty_out").cast(DecimalType(18,2)).alias("total_qty_out"),
            (sum("qty_in") - sum("qty_out")).cast(DecimalType(18,2)).alias("net_quantity"),
            
            # Value flows
            sum("value_in").cast(DecimalType(18,2)).alias("total_value_in"),
            sum("value_out").cast(DecimalType(18,2)).alias("total_value_out"),
            (sum("value_in") - sum("value_out")).cast(DecimalType(18,2)).alias("net_inventory_value"),
            
            # Legacy quantity column for backward compatibility
            sum("quantity").cast(DecimalType(18,2)).alias("total_quantity"),
            
            # Expired stock (items with expiry_date < today)
            sum(when(col("is_expired"), col("qty_in") - col("qty_out")).otherwise(lit(0))).cast(DecimalType(18,2)).alias("expired_qty"),
            sum(when(col("is_expired"), col("value_in") - col("value_out")).otherwise(lit(0))).cast(DecimalType(18,2)).alias("expired_value"),
            
            # Get location for grouping
            first("location_key").alias("location_key"),
            first("inventory_fact_key").alias("inventory_key")
        )
    
    # Calculate cumulative inventory over time
    window_spec = Window.orderBy("date_key")
    
    # Cumulative quantity
    inv_agg = inv_agg.withColumn(
        "ending_inventory_qty",
        sum("net_quantity").over(window_spec).cast(DecimalType(18,2))
    )
    
    # Cumulative value
    inv_agg = inv_agg.withColumn(
        "ending_inventory_value",
        sum("net_inventory_value").over(window_spec).cast(DecimalType(18,2))
    )
    
    # Calculate beginning inventory (lag)
    inv_agg = inv_agg.withColumn(
        "beginning_inventory_qty",
        coalesce(lag("ending_inventory_qty", 1).over(window_spec), lit(0)).cast(DecimalType(18,2))
    ).withColumn(
        "beginning_inventory_value",
        coalesce(lag("ending_inventory_value", 1).over(window_spec), lit(0)).cast(DecimalType(18,2))
    )
    
    # Calculate average inventory
    inv_agg = inv_agg.withColumn(
        "average_inventory_qty",
        ((col("beginning_inventory_qty") + col("ending_inventory_qty")) / 2).cast(DecimalType(18,2))
    ).withColumn(
        "average_inventory_value",
        ((col("beginning_inventory_value") + col("ending_inventory_value")) / 2).cast(DecimalType(18,2))
    )
    
    # COGS = value_out (goods leaving inventory)
    inv_agg = inv_agg.withColumn(
        "cogs_mtd",
        col("total_value_out").cast(DecimalType(18,2))
    )
    
    # Inventory Turnover Ratio = COGS / Average Inventory Value
    inv_agg = inv_agg.withColumn(
        "inventory_turnover_ratio",
        when(col("average_inventory_value") > 0,
             col("cogs_mtd") / col("average_inventory_value")
        ).otherwise(lit(0)).cast(DecimalType(18,2))
    )
    
    # Days Inventory Outstanding = 30 / Turnover Ratio
    inv_agg = inv_agg.withColumn(
        "days_inventory_outstanding",
        when(col("inventory_turnover_ratio") > 0,
             lit(30) / col("inventory_turnover_ratio")
        ).otherwise(lit(999)).cast(DecimalType(18,2))
    )
    
    # Expired stock metrics (cumulative)
    inv_agg = inv_agg.withColumn(
        "expired_stock_quantity",
        sum("expired_qty").over(window_spec).cast(LongType())
    ).withColumn(
        "expired_stock_value",
        sum("expired_value").over(window_spec).cast(DecimalType(18,2))
    ).withColumn(
        "total_stock_quantity",
        col("ending_inventory_qty").cast(LongType())
    ).withColumn(
        "total_stock_value",
        col("ending_inventory_value")
    ).withColumn(
        # Expired stock percentage = expired qty / total qty * 100
        "expired_stock_percentage",
        when(col("ending_inventory_qty") > 0,
             (col("expired_stock_quantity") / col("ending_inventory_qty") * 100)
        ).otherwise(lit(0)).cast(FloatType())
    ).withColumn(
        "category", lit("All").cast(StringType())
    )
    
    # Calculate health scores
    # Turnover score: Higher turnover = better (capped at 100)
    inv_agg = inv_agg.withColumn(
        "inventory_turnover_score",
        least(lit(100), col("inventory_turnover_ratio") * 20).cast(FloatType())
    ).withColumn(
        # Expired stock score: Lower expired % = better
        "expired_stock_score",
        greatest(lit(0), (lit(100) - col("expired_stock_percentage"))).cast(FloatType())
    ).withColumn(
        # DIO score: Lower DIO = better (100 - DIO, min 0, max 100)
        "ccc_inventory_score",
        greatest(lit(0), least(lit(100), lit(100) - col("days_inventory_outstanding"))).cast(FloatType())
    ).withColumn(
        # Weighted inventory health score
        "inventory_health_score",
        (
            col("inventory_turnover_score") * 0.4 +
            col("expired_stock_score") * 0.3 +
            col("ccc_inventory_score") * 0.3
        ).cast(FloatType())
    )
    
    # Add metadata
    inv_agg = inv_agg \
        .withColumn("invoice_date_key", col("date_key")) \
        .withColumn("invoice_id", lit(None).cast(StringType())) \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # Select final columns matching target schema
    result = inv_agg.select(
        col("average_inventory_value").cast(DecimalType(18,2)),
        col("beginning_inventory_value").cast(DecimalType(18,2)),
        col("category").cast(StringType()),
        col("ccc_inventory_score").cast(FloatType()),
        col("cogs_mtd").cast(DecimalType(18,2)),
        col("days_inventory_outstanding").cast(DecimalType(18,2)),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("ending_inventory_value").cast(DecimalType(18,2)),
        col("expired_stock_percentage").cast(FloatType()),
        col("expired_stock_quantity").cast(LongType()),
        col("expired_stock_score").cast(FloatType()),
        col("expired_stock_value").cast(DecimalType(18,2)),
        col("inventory_health_score").cast(FloatType()),
        col("inventory_key").cast(LongType()),
        col("inventory_turnover_ratio").cast(DecimalType(18,2)),
        col("inventory_turnover_score").cast(FloatType()),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("total_stock_quantity").cast(LongType()),
        col("total_stock_value").cast(DecimalType(18,2))
    )
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(create_agg_inventory())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Pipeline
# 
# Run all steps in sequence: Bronze → Silver → Gold Fact → Gold Agg

# CELL ********************

start_job_instance(batch_id, job_id, msg="Start Inventory Pipeline")

try:
    # ========== STEP 1: Silver Adjustment ==========
    print("Step 1/4: Creating silver_adjustment...")
    df_silver_adj = transform_silver_adjustment()
    adj_src_cnt = spark.table(BRONZE_TABLES["adjustment"]).count()
    adj_tgt_cnt = df_silver_adj.count()
    
    target_path_adj = f"{lh_silver_path}/silver_adjustment"
    df_silver_adj.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path_adj)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES['adjustment']} USING DELTA LOCATION '{target_path_adj}'")
    print(f"  ✅ silver_adjustment: {adj_src_cnt} → {adj_tgt_cnt} rows")
    
    # ========== STEP 2: Silver Delivery Order ==========
    print("Step 2/4: Creating silver_deliveryorder...")
    df_silver_do = transform_silver_deliveryorder()
    do_src_cnt = spark.table(BRONZE_TABLES["deliveryorder"]).count()
    do_tgt_cnt = df_silver_do.count()
    
    target_path_do = f"{lh_silver_path}/silver_deliveryorder"
    df_silver_do.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path_do)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES['deliveryorder']} USING DELTA LOCATION '{target_path_do}'")
    print(f"  ✅ silver_deliveryorder: {do_src_cnt} → {do_tgt_cnt} rows")
    
    # ========== STEP 3: Gold Fact Inventory ==========
    print("Step 3/4: Creating fact_inventory...")
    df_fact = create_fact_inventory()
    fact_tgt_cnt = df_fact.count()
    
    df_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_FACTS["inventory"])
    print(f"  ✅ fact_inventory: {adj_tgt_cnt + do_tgt_cnt} → {fact_tgt_cnt} rows")
    
    # ========== STEP 4: Gold Agg Inventory ==========
    print("Step 4/4: Creating agg_inventory...")
    df_agg = create_agg_inventory()
    agg_tgt_cnt = df_agg.count()
    
    df_agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(AGG_TABLES["inventory"])
    print(f"  ✅ agg_inventory: {fact_tgt_cnt} → {agg_tgt_cnt} rows")
    
    # ========== SUMMARY ==========
    total_src = adj_src_cnt + do_src_cnt
    end_job_instance(batch_id, job_id, "SUCCESS", 
                     msg=f"Inventory Pipeline Complete: silver_adj={adj_tgt_cnt}, silver_do={do_tgt_cnt}, fact={fact_tgt_cnt}, agg={agg_tgt_cnt}",
                     src_row_num=total_src, tgt_row_num=agg_tgt_cnt)
    
    print("\n" + "="*50)
    print("INVENTORY PIPELINE COMPLETE")
    print("="*50)
    print(f"Bronze Adjustment    → Silver: {adj_src_cnt:,} → {adj_tgt_cnt:,}")
    print(f"Bronze DeliveryOrder → Silver: {do_src_cnt:,} → {do_tgt_cnt:,}")
    print(f"Silver → Gold Fact:            {fact_tgt_cnt:,}")
    print(f"Gold Fact → Gold Agg:          {agg_tgt_cnt:,}")
    print("="*50)
    
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Inventory Pipeline Failed: {safe_exception_text(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Preview Results (Optional)

# CELL ********************

# Preview agg_inventory results
display(spark.table(AGG_TABLES["inventory"]).orderBy(col("invoice_date_key").desc()).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check inventory health metrics
spark.table(AGG_TABLES["inventory"]).select(
    "invoice_date_key",
    "total_stock_quantity",
    "total_stock_value",
    "expired_stock_quantity",
    "expired_stock_percentage",
    "inventory_turnover_ratio",
    "days_inventory_outstanding",
    "inventory_health_score"
).orderBy(col("invoice_date_key").desc()).show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
