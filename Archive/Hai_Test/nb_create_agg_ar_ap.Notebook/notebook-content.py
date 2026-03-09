# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a",
# META       "default_lakehouse_name": "lh_bnj_silver",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
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

# # Create agg_ar_ap Aggregate Table
# 
# This notebook creates the `agg_ar_ap` table following the exact schema from `lh_bnj_gold`.
# 
# ## Target Schema: `lh_bnj_gold.dbo.agg_ar_ap`
# 
# | Column | Data Type | Description |
# |--------|-----------|-------------|
# | invoice_id | varchar | Invoice identifier |
# | invoice_date_key | int | FK to dim_date (invoice date) |
# | date_key | int | FK to dim_date (report/snapshot date) |
# | report_date | date | Report as-of date |
# | corporate_id | varchar | Corporate identifier |
# | inventory_key | bigint | FK to dim_inventory |
# | location_key | bigint | FK to dim_location |
# | ar_*_amount | decimal | AR aging bucket amounts |
# | ar_*_count | bigint | AR aging bucket counts |
# | ap_*_amount | real | AP aging bucket amounts |
# | ap_*_count | int | AP aging bucket counts |
# | *_days_outstanding | real | Average days outstanding |
# | *_aging_score | real | Aging health score |
# | ap_paid_*_count | int | Payment timing metrics |
# | dw_created_at | datetime2 | Record creation timestamp |
# | dw_updated_at | datetime2 | Record update timestamp |


# MARKDOWN ********************

# ## Configuration

# CELL ********************

# Configuration
SILVER_SCHEMA = "lh_bnj_silver.xero"
GOLD_CATALOG = "lh_bnj_gold"
GOLD_SCHEMA = "gold_test"  # Target schema
TABLE_NAME = "agg_ar_ap"

# Full table path
TARGET_TABLE = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{TABLE_NAME}"

print(f"Source: {SILVER_SCHEMA}")
print(f"Target: {TARGET_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Define Schema

# CELL ********************

from pyspark.sql.types import *

# Define exact schema matching lh_bnj_gold.dbo.agg_ar_ap
AGG_AR_AP_SCHEMA = StructType([
    # Identifiers
    StructField("invoice_id", StringType(), True),
    StructField("invoice_date_key", IntegerType(), True),
    StructField("date_key", IntegerType(), True),
    StructField("report_date", DateType(), True),
    StructField("corporate_id", StringType(), True),
    StructField("payer_key", LongType(), True),  # FK to dim_payer (for corporate/insurance)
    StructField("inventory_key", LongType(), True),
    StructField("location_key", LongType(), True),
    
    # AR Aging Amounts (decimal)
    StructField("ar_0_30_days_amount", DecimalType(18, 2), True),
    StructField("ar_31_60_days_amount", DecimalType(18, 2), True),
    StructField("ar_61_90_days_amount", DecimalType(18, 2), True),
    StructField("ar_91_180_days_amount", DecimalType(18, 2), True),
    StructField("ar_over_180_days_amount", DecimalType(18, 2), True),
    StructField("total_ar_amount", DecimalType(18, 2), True),
    
    # AR Aging Counts (bigint)
    StructField("ar_0_30_days_count", LongType(), True),
    StructField("ar_31_60_days_count", LongType(), True),
    StructField("ar_61_90_days_count", LongType(), True),
    StructField("ar_91_180_days_count", LongType(), True),
    StructField("ar_over_180_days_count", LongType(), True),
    
    # AP Aging Amounts (real/float)
    StructField("ap_0_30_days_amount", FloatType(), True),
    StructField("ap_31_60_days_amount", FloatType(), True),
    StructField("ap_61_90_days_amount", FloatType(), True),
    StructField("ap_91_180_days_amount", FloatType(), True),
    StructField("ap_over_180_days_amount", FloatType(), True),
    StructField("total_ap_amount", FloatType(), True),
    
    # AP Aging Counts (int)
    StructField("ap_0_30_days_count", IntegerType(), True),
    StructField("ap_31_60_days_count", IntegerType(), True),
    StructField("ap_61_90_days_count", IntegerType(), True),
    StructField("ap_91_180_days_count", IntegerType(), True),
    StructField("ap_over_180_days_count", IntegerType(), True),
    
    # Metrics
    StructField("ar_days_outstanding", FloatType(), True),
    StructField("ap_days_outstanding", FloatType(), True),
    StructField("ar_aging_score", FloatType(), True),
    StructField("ap_aging_score", FloatType(), True),
    StructField("ar_ap_health_score", FloatType(), True),
    
    # AP Payment Timing
    StructField("ap_paid_early_count", IntegerType(), True),
    StructField("ap_paid_on_time_count", IntegerType(), True),
    StructField("ap_paid_late_count", IntegerType(), True),
    
    # Audit columns
    StructField("dw_created_at", TimestampType(), True),
    StructField("dw_updated_at", TimestampType(), True)
])

print(f"Schema defined with {len(AGG_AR_AP_SCHEMA.fields)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Read Source Data from Silver Layer

# CELL ********************

GOLD_SCHEMA_FULL = "lh_bnj_gold.gold_test"
df_payer_mapping = spark.sql(f"""
        SELECT DISTINCT
            fp.invoice_id,
            fp.payer_key,
            p.payer_type
        FROM {GOLD_SCHEMA_FULL}.fact_payment fp
        LEFT JOIN {GOLD_SCHEMA_FULL}.dim_payer p ON fp.payer_key = p.payer_id
    """)
display(df_payer_mapping)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from datetime import date

# Gold schema for dim_payer and fact_payments
GOLD_SCHEMA_FULL = "lh_bnj_gold.gold_test"

# Read invoices from silver layer
df_invoices = spark.sql(f"""
    SELECT 
        invoice_id,
        contact_id,
        contact_name,
        type,
        status,
        DATE(invoice_date) as invoice_date,
        DATE(due_date) as due_date,
        CAST(amount_due AS DECIMAL(18,2)) as amount_due,
        CAST(total AS DECIMAL(18,2)) as total_amount
    FROM {SILVER_SCHEMA}.silver_invoices
    WHERE status IN ('AUTHORISED', 'PAID')
      AND invoice_date IS NOT NULL
""")

print(f"Total invoices: {df_invoices.count()}")
print(f"AR (ACCREC): {df_invoices.filter(col('type') == 'ACCREC').count()}")
print(f"AP (ACCPAY): {df_invoices.filter(col('type') == 'ACCPAY').count()}")

# Try to read payer_key from fact_payments joined with dim_payer
try:
    df_payer_mapping = spark.sql(f"""
        SELECT DISTINCT
            fp.invoice_id,
            fp.payer_key,
            p.payer_type
        FROM {GOLD_SCHEMA_FULL}.fact_payments fp
        LEFT JOIN {GOLD_SCHEMA_FULL}.dim_payer p ON fp.payer_key = p.payer_id
    """)
    display(df_payer_mapping)
    HAS_PAYER_DATA = True
    print(f"\nPayer mapping available: {df_payer_mapping.count()} invoice-payer records")
    print(f"Corporate (Insurance) invoices: {df_payer_mapping.filter(col('payer_type') == 'Insurance').count()}")
except Exception as e:
    HAS_PAYER_DATA = False
    df_payer_mapping = None
    print(f"\nPayer data not available: {e}")
    print("payer_key will be NULL")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Calculate Aging Buckets and Metrics

# CELL ********************

# Use current date as reference for aging calculation
reference_date = current_date()

# Add calculated columns
df_with_aging = df_invoices \
    .withColumn("days_overdue", datediff(reference_date, col("due_date"))) \
    .withColumn("invoice_date_key", date_format(col("invoice_date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("date_key", date_format(reference_date, "yyyyMMdd").cast(IntegerType())) \
    .withColumn("report_date", reference_date)

# Join with payer data to get payer_key (if available)
if HAS_PAYER_DATA and df_payer_mapping is not None:
    df_with_aging = df_with_aging.join(
        df_payer_mapping.select("invoice_id", "payer_key"),
        on="invoice_id",
        how="left"
    )
    print("Joined with payer data for payer_key")
else:
    df_with_aging = df_with_aging.withColumn("payer_key", lit(None).cast(LongType()))
    print("No payer data - payer_key will be NULL")

# Payment timing will be calculated based on status (PAID vs AUTHORISED)
# Since fully_paid_on_date is not available, we use status to determine payment timing
df_with_aging = df_with_aging \
    .withColumn("is_paid", when(col("status") == "PAID", lit(True)).otherwise(lit(False)))

print("Sample data with aging:")
df_with_aging.select("invoice_id", "type", "due_date", "days_overdue", "amount_due", "payer_key").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# ACCOUNTS RECEIVABLE (ACCREC)
# =====================================================
df_ar = df_with_aging.filter(col("type") == "ACCREC") \
    .withColumn("ar_0_30_days_amount", 
        when(col("days_overdue") <= 30, col("amount_due")).otherwise(lit(0)).cast(DecimalType(18,2))) \
    .withColumn("ar_31_60_days_amount", 
        when((col("days_overdue") > 30) & (col("days_overdue") <= 60), col("amount_due")).otherwise(lit(0)).cast(DecimalType(18,2))) \
    .withColumn("ar_61_90_days_amount", 
        when((col("days_overdue") > 60) & (col("days_overdue") <= 90), col("amount_due")).otherwise(lit(0)).cast(DecimalType(18,2))) \
    .withColumn("ar_91_180_days_amount", 
        when((col("days_overdue") > 90) & (col("days_overdue") <= 180), col("amount_due")).otherwise(lit(0)).cast(DecimalType(18,2))) \
    .withColumn("ar_over_180_days_amount", 
        when(col("days_overdue") > 180, col("amount_due")).otherwise(lit(0)).cast(DecimalType(18,2))) \
    .withColumn("total_ar_amount", col("amount_due").cast(DecimalType(18,2))) \
    .withColumn("ar_0_30_days_count", when(col("days_overdue") <= 30, lit(1)).otherwise(lit(0)).cast(LongType())) \
    .withColumn("ar_31_60_days_count", when((col("days_overdue") > 30) & (col("days_overdue") <= 60), lit(1)).otherwise(lit(0)).cast(LongType())) \
    .withColumn("ar_61_90_days_count", when((col("days_overdue") > 60) & (col("days_overdue") <= 90), lit(1)).otherwise(lit(0)).cast(LongType())) \
    .withColumn("ar_91_180_days_count", when((col("days_overdue") > 90) & (col("days_overdue") <= 180), lit(1)).otherwise(lit(0)).cast(LongType())) \
    .withColumn("ar_over_180_days_count", when(col("days_overdue") > 180, lit(1)).otherwise(lit(0)).cast(LongType())) \
    .withColumn("ar_days_outstanding", col("days_overdue").cast(FloatType())) \
    .withColumn("ar_aging_score", 
        (100 - least(col("days_overdue"), lit(100))).cast(FloatType()))  # 100 = current, 0 = 100+ days

print(f"AR records: {df_ar.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# ACCOUNTS PAYABLE (ACCPAY)
# =====================================================
df_ap = df_with_aging.filter(col("type") == "ACCPAY") \
    .withColumn("ap_0_30_days_amount", 
        when(col("days_overdue") <= 30, col("amount_due")).otherwise(lit(0)).cast(FloatType())) \
    .withColumn("ap_31_60_days_amount", 
        when((col("days_overdue") > 30) & (col("days_overdue") <= 60), col("amount_due")).otherwise(lit(0)).cast(FloatType())) \
    .withColumn("ap_61_90_days_amount", 
        when((col("days_overdue") > 60) & (col("days_overdue") <= 90), col("amount_due")).otherwise(lit(0)).cast(FloatType())) \
    .withColumn("ap_91_180_days_amount", 
        when((col("days_overdue") > 90) & (col("days_overdue") <= 180), col("amount_due")).otherwise(lit(0)).cast(FloatType())) \
    .withColumn("ap_over_180_days_amount", 
        when(col("days_overdue") > 180, col("amount_due")).otherwise(lit(0)).cast(FloatType())) \
    .withColumn("total_ap_amount", col("amount_due").cast(FloatType())) \
    .withColumn("ap_0_30_days_count", when(col("days_overdue") <= 30, lit(1)).otherwise(lit(0)).cast(IntegerType())) \
    .withColumn("ap_31_60_days_count", when((col("days_overdue") > 30) & (col("days_overdue") <= 60), lit(1)).otherwise(lit(0)).cast(IntegerType())) \
    .withColumn("ap_61_90_days_count", when((col("days_overdue") > 60) & (col("days_overdue") <= 90), lit(1)).otherwise(lit(0)).cast(IntegerType())) \
    .withColumn("ap_91_180_days_count", when((col("days_overdue") > 90) & (col("days_overdue") <= 180), lit(1)).otherwise(lit(0)).cast(IntegerType())) \
    .withColumn("ap_over_180_days_count", when(col("days_overdue") > 180, lit(1)).otherwise(lit(0)).cast(IntegerType())) \
    .withColumn("ap_days_outstanding", col("days_overdue").cast(FloatType())) \
    .withColumn("ap_aging_score", 
        (100 - least(col("days_overdue"), lit(100))).cast(FloatType())) \
    .withColumn("ap_paid_early_count", 
        when((col("is_paid") == True) & (col("days_overdue") < 0), lit(1)).otherwise(lit(0)).cast(IntegerType())) \
    .withColumn("ap_paid_on_time_count", 
        when((col("is_paid") == True) & (col("days_overdue") >= 0) & (col("days_overdue") <= 30), lit(1)).otherwise(lit(0)).cast(IntegerType())) \
    .withColumn("ap_paid_late_count", 
        when((col("is_paid") == True) & (col("days_overdue") > 30), lit(1)).otherwise(lit(0)).cast(IntegerType()))

print(f"AP records: {df_ap.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Combine AR and AP into Final Table

# CELL ********************

# Select AR columns with null AP columns
df_ar_final = df_ar.select(
    col("invoice_id").cast(StringType()),
    col("invoice_date_key").cast(IntegerType()),
    col("date_key").cast(IntegerType()),
    col("report_date").cast(DateType()),
    lit(None).cast(StringType()).alias("corporate_id"),
    col("payer_key").cast(LongType()),  # FK to dim_payer
    lit(None).cast(LongType()).alias("inventory_key"),
    lit(None).cast(LongType()).alias("location_key"),
    # AR amounts
    col("ar_0_30_days_amount"),
    col("ar_31_60_days_amount"),
    col("ar_61_90_days_amount"),
    col("ar_91_180_days_amount"),
    col("ar_over_180_days_amount"),
    col("total_ar_amount"),
    # AR counts
    col("ar_0_30_days_count"),
    col("ar_31_60_days_count"),
    col("ar_61_90_days_count"),
    col("ar_91_180_days_count"),
    col("ar_over_180_days_count"),
    # AP amounts (zeros for AR records)
    lit(0.0).cast(FloatType()).alias("ap_0_30_days_amount"),
    lit(0.0).cast(FloatType()).alias("ap_31_60_days_amount"),
    lit(0.0).cast(FloatType()).alias("ap_61_90_days_amount"),
    lit(0.0).cast(FloatType()).alias("ap_91_180_days_amount"),
    lit(0.0).cast(FloatType()).alias("ap_over_180_days_amount"),
    lit(0.0).cast(FloatType()).alias("total_ap_amount"),
    # AP counts (zeros for AR records)
    lit(0).cast(IntegerType()).alias("ap_0_30_days_count"),
    lit(0).cast(IntegerType()).alias("ap_31_60_days_count"),
    lit(0).cast(IntegerType()).alias("ap_61_90_days_count"),
    lit(0).cast(IntegerType()).alias("ap_91_180_days_count"),
    lit(0).cast(IntegerType()).alias("ap_over_180_days_count"),
    # Metrics
    col("ar_days_outstanding"),
    lit(0.0).cast(FloatType()).alias("ap_days_outstanding"),
    col("ar_aging_score"),
    lit(0.0).cast(FloatType()).alias("ap_aging_score"),
    col("ar_aging_score").alias("ar_ap_health_score"),  # Use AR score as health score
    # AP payment timing (zeros for AR)
    lit(0).cast(IntegerType()).alias("ap_paid_early_count"),
    lit(0).cast(IntegerType()).alias("ap_paid_on_time_count"),
    lit(0).cast(IntegerType()).alias("ap_paid_late_count"),
    # Audit columns
    current_timestamp().alias("dw_created_at"),
    current_timestamp().alias("dw_updated_at")
)

print(f"AR final records: {df_ar_final.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select AP columns with null AR columns
df_ap_final = df_ap.select(
    col("invoice_id").cast(StringType()),
    col("invoice_date_key").cast(IntegerType()),
    col("date_key").cast(IntegerType()),
    col("report_date").cast(DateType()),
    lit(None).cast(StringType()).alias("corporate_id"),
    col("payer_key").cast(LongType()),  # FK to dim_payer
    lit(None).cast(LongType()).alias("inventory_key"),
    lit(None).cast(LongType()).alias("location_key"),
    # AR amounts (zeros for AP records)
    lit(0).cast(DecimalType(18,2)).alias("ar_0_30_days_amount"),
    lit(0).cast(DecimalType(18,2)).alias("ar_31_60_days_amount"),
    lit(0).cast(DecimalType(18,2)).alias("ar_61_90_days_amount"),
    lit(0).cast(DecimalType(18,2)).alias("ar_91_180_days_amount"),
    lit(0).cast(DecimalType(18,2)).alias("ar_over_180_days_amount"),
    lit(0).cast(DecimalType(18,2)).alias("total_ar_amount"),
    # AR counts (zeros for AP records)
    lit(0).cast(LongType()).alias("ar_0_30_days_count"),
    lit(0).cast(LongType()).alias("ar_31_60_days_count"),
    lit(0).cast(LongType()).alias("ar_61_90_days_count"),
    lit(0).cast(LongType()).alias("ar_91_180_days_count"),
    lit(0).cast(LongType()).alias("ar_over_180_days_count"),
    # AP amounts
    col("ap_0_30_days_amount"),
    col("ap_31_60_days_amount"),
    col("ap_61_90_days_amount"),
    col("ap_91_180_days_amount"),
    col("ap_over_180_days_amount"),
    col("total_ap_amount"),
    # AP counts
    col("ap_0_30_days_count"),
    col("ap_31_60_days_count"),
    col("ap_61_90_days_count"),
    col("ap_91_180_days_count"),
    col("ap_over_180_days_count"),
    # Metrics
    lit(0.0).cast(FloatType()).alias("ar_days_outstanding"),
    col("ap_days_outstanding"),
    lit(0.0).cast(FloatType()).alias("ar_aging_score"),
    col("ap_aging_score"),
    col("ap_aging_score").alias("ar_ap_health_score"),  # Use AP score as health score
    # AP payment timing
    col("ap_paid_early_count"),
    col("ap_paid_on_time_count"),
    col("ap_paid_late_count"),
    # Audit columns
    current_timestamp().alias("dw_created_at"),
    current_timestamp().alias("dw_updated_at")
)

print(f"AP final records: {df_ap_final.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Union AR and AP DataFrames
df_agg_ar_ap = df_ar_final.union(df_ap_final)

print(f"\n=== Combined agg_ar_ap ===")
print(f"Total records: {df_agg_ar_ap.count()}")
print(f"\nSchema:")
df_agg_ar_ap.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Save to Gold Layer

# CELL ********************

# Option 2: Create/Replace managed table in catalog (uncomment to use)
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{GOLD_SCHEMA}")

df_agg_ar_ap.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"✓ Saved to catalog table: {TARGET_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary
# 
# Created `agg_ar_ap` table with the following columns:
# 
# | Category | Columns |
# |----------|--------|
# | **Identifiers** | invoice_id, invoice_date_key, date_key, report_date, corporate_id, inventory_key, location_key |
# | **AR Amounts** | ar_0_30_days_amount, ar_31_60_days_amount, ar_61_90_days_amount, ar_91_180_days_amount, ar_over_180_days_amount, total_ar_amount |
# | **AR Counts** | ar_0_30_days_count, ar_31_60_days_count, ar_61_90_days_count, ar_91_180_days_count, ar_over_180_days_count |
# | **AP Amounts** | ap_0_30_days_amount, ap_31_60_days_amount, ap_61_90_days_amount, ap_91_180_days_amount, ap_over_180_days_amount, total_ap_amount |
# | **AP Counts** | ap_0_30_days_count, ap_31_60_days_count, ap_61_90_days_count, ap_91_180_days_count, ap_over_180_days_count |
# | **Metrics** | ar_days_outstanding, ap_days_outstanding, ar_aging_score, ap_aging_score, ar_ap_health_score |
# | **AP Payment** | ap_paid_early_count, ap_paid_on_time_count, ap_paid_late_count |
# | **Audit** | dw_created_at, dw_updated_at |

