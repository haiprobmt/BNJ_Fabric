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

# # Gold Aggregation: `agg_inventory` (Single Table)


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260226091200
job_id = '5071'
src_catalog = "gold"
job_group_name = "gold_agg"
src_table = ""
tgt_catalog = "gold"

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

#== DEFINE RELATE TABLES ==
SILVER_SCHEMA_PLATO = "lh_bnj_silver.plato"
SILVER_SCHEMA_XERO = "lh_bnj_silver.xero"
GOLD_SCHEMA = f"lh_bnj_gold.{tgt_catalog}"

GOLD_FACTS = {"inventory": f"`WS-ETL-BNJ`.lh_bnj_gold.{tgt_catalog}.fact_inventory"}
AGG_TABLES = {"inventory": f"`WS-ETL-BNJ`.lh_bnj_gold.{tgt_catalog}.agg_inventory"}
GOLD_DIMENSIONS = {"date": f"`WS-ETL-BNJ`lh_bnj_gold.{tgt_catalog}.dim_date"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(GOLD_FACTS['inventory'])
print(AGG_TABLES['inventory'])
print(GOLD_DIMENSIONS['date'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
    
    # Calculate health scores using dynamic targets from brz_target
    targets = get_targets_for_period()
    MIN_ITR = targets.get("min_inventory_turnover_rate", 4.0)
    MAX_ITR = targets.get("max_inventory_turnover_rate", 10.0)
    MAX_EXPIRED_PCT = targets.get("max_expired_stock_percentage", 3.0)
    
    # Turnover score: Score based on ITR within target range
    # Score = 100 if ITR >= MAX_ITR, Score = 0 if ITR < MIN_ITR
    inv_agg = inv_agg.withColumn(
        "inventory_turnover_score",
        when(col("inventory_turnover_ratio") >= MAX_ITR, lit(100))
        .when(col("inventory_turnover_ratio") <= MIN_ITR, lit(0))
        .otherwise(
            ((col("inventory_turnover_ratio") - MIN_ITR) / (MAX_ITR - MIN_ITR) * 100)
        ).cast(FloatType())
    ).withColumn(
        # Expired stock score: Lower expired % = better (100 if 0%, 0 if >= MAX_EXPIRED_PCT)
        "expired_stock_score",
        when(col("expired_stock_percentage") <= 0, lit(100))
        .when(col("expired_stock_percentage") >= MAX_EXPIRED_PCT, lit(0))
        .otherwise(
            lit(100) - (col("expired_stock_percentage") / MAX_EXPIRED_PCT * 100)
        ).cast(FloatType())
    ).withColumn(
        # DIO score: Lower DIO = better (100 - DIO, min 0, max 100)
        "ccc_inventory_score",
        greatest(lit(0), least(lit(100), lit(100) - col("days_inventory_outstanding"))).cast(FloatType())
    ).withColumn(
        # Weighted inventory health score using weights from Formular Calculations doc
        # Formula: Inv_Health = 0.40×ITR + 0.20×Expired + 0.40×CCC
        "inventory_health_score",
        (
            col("inventory_turnover_score") * INVENTORY_WEIGHTS["itr_score"] +     # 0.40
            col("expired_stock_score") * INVENTORY_WEIGHTS["expired_score"] +      # 0.20
            col("ccc_inventory_score") * INVENTORY_WEIGHTS["ccc_score"]            # 0.40
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

# #start_job_instance(batch_id, job_id, msg="Start agg_inventory")
# try:
#     df = create_agg_inventory()
#     src_cnt = spark.table(GOLD_FACTS["inventory"]).count()
#     tgt_cnt = df.count()
#     log_data_quality("agg_inventory", df, "inventory_key")

#     tgt_table = AGG_TABLES["inventory"]

#     # Ensure table exists before overwrite
#     spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {tgt_table}
#     USING DELTA
#     """)

#     df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tgt_table)

#     end_job_instance(batch_id, job_id, "SUCCESS", msg="Created agg_inventory", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
#     print(f"✅ Created {tgt_table} with {src_cnt} rows")
#     print("OK")
# except Exception as e:
#     end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed agg_inventory. {safe_exception_text(e)}")
#     raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F

try:
    df = create_agg_inventory()
    src_cnt = spark.table(GOLD_FACTS["inventory"]).count()
    tgt_table = AGG_TABLES["inventory"]

    key_cols = ["invoice_date_key"]

    dup_cnt = (
        df.groupBy(*key_cols).count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dup_cnt > 0:
        raise Exception(f"Source DF has {dup_cnt} duplicate key groups for {key_cols}")

    if df.filter(F.col("invoice_date_key").isNull()).limit(1).count() > 0:
        raise Exception("NULL invoice_date_key found in source DF")

    if not spark.catalog.tableExists(tgt_table):
        df.write.format("delta").mode("overwrite").saveAsTable(tgt_table)
    else:
        DeltaTable.forName(spark, tgt_table) \
            .alias("t") \
            .merge(df.alias("s"), "t.invoice_date_key = s.invoice_date_key") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    final_tgt_cnt = spark.table(tgt_table).count()
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Merged agg_inventory",
                     src_row_num=src_cnt, tgt_row_num=final_tgt_cnt)
    print("OK")

except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED",
                     msg=f"Failed agg_inventory. {safe_exception_text(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # LECACY LOGIC

# CELL ********************

def create_agg_inventory():
    """
    Create agg_inventory aggregate table for Inventory Health dashboard.
    Tracks turnover ratio, expired stock, and DIO metrics.
    """
    
    # Read fact_inventory
    fact_inventory = spark.table(GOLD_FACTS["inventory"])
    dim_date = spark.table(GOLD_DIMENSIONS["date"])
    
    # Aggregate inventory movements by date
    inv_agg = fact_inventory.groupBy("date_key") \
        .agg(
            # Stock values
            sum("total_value").cast(DecimalType(18,2)).alias("ending_inventory_value"),
            sum("quantity_in").alias("total_quantity_in"),
            sum("quantity_out").alias("total_quantity_out"),
            (sum("quantity_in") - sum("quantity_out")).alias("net_stock_quantity"),
            
            # Get location for grouping
            first("location_key").alias("location_key"),
            first("inventory_fact_key").alias("inventory_key")
        )
    
    # Calculate beginning inventory (lag of ending inventory)
    window_spec = Window.orderBy("date_key")
    inv_agg = inv_agg.withColumn(
        "beginning_inventory_value",
        lag("ending_inventory_value", 1).over(window_spec)
    ).withColumn(
        "beginning_inventory_value",
        coalesce(col("beginning_inventory_value"), col("ending_inventory_value"))
    )
    
    # Calculate average inventory
    inv_agg = inv_agg.withColumn(
        "average_inventory_value",
        ((col("beginning_inventory_value") + col("ending_inventory_value")) / 2).cast(DecimalType(18,2))
    )
    
    # COGS approximation (value of goods sold = quantity_out * avg cost)
    inv_agg = inv_agg.withColumn(
        "cogs_mtd",
        (col("total_quantity_out") * (col("ending_inventory_value") / greatest(col("net_stock_quantity"), lit(1)))).cast(DecimalType(18,2))
    )
    
    # Inventory Turnover Ratio
    inv_agg = inv_agg.withColumn(
        "inventory_turnover_ratio",
        (col("cogs_mtd") / greatest(col("average_inventory_value"), lit(1))).cast(DecimalType(18,2))
    )
    
    # Days Inventory Outstanding
    inv_agg = inv_agg.withColumn(
        "days_inventory_outstanding",
        (lit(30) / greatest(col("inventory_turnover_ratio"), lit(0.01))).cast(DecimalType(18,2))
    )
    
    # Expired stock (placeholder - would need expiry tracking)
    inv_agg = inv_agg.withColumn(
        "expired_stock_quantity", lit(0).cast(LongType())
    ).withColumn(
        "expired_stock_value", lit(0).cast(DecimalType(18,2))
    ).withColumn(
        "expired_stock_percentage", lit(0).cast(FloatType())
    ).withColumn(
        "total_stock_quantity", col("net_stock_quantity").cast(LongType())
    ).withColumn(
        "total_stock_value", col("ending_inventory_value")
    ).withColumn(
        "category", lit("All").cast(StringType())
    )
    
    # Calculate health scores
    inv_agg = inv_agg.withColumn(
        "inventory_turnover_score",
        least(lit(100), col("inventory_turnover_ratio") * 20).cast(FloatType())
    ).withColumn(
        "expired_stock_score",
        (lit(100) - col("expired_stock_percentage")).cast(FloatType())
    ).withColumn(
        "ccc_inventory_score",
        greatest(lit(0), lit(100) - col("days_inventory_outstanding")).cast(FloatType())
    ).withColumn(
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
