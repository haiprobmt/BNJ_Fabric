# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2aae23b9-c116-40b8-9787-a7c48707bf50",
# META       "default_lakehouse_name": "lh_bnj_metadata",
# META       "default_lakehouse_workspace_id": "87076c77-5525-4288-9ae6-8631261bdbd5",
# META       "known_lakehouses": [
# META         {
# META           "id": "2aae23b9-c116-40b8-9787-a7c48707bf50"
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

# # Gold Aggregation: `agg_ar_ap` (Single Table)


# PARAMETERS CELL ********************

batch_id = 20260209125800
job_id = '5464'
src_table = 'silver_payment'
src_catalog = 'xero'
tgt_table = 'agg_ar_ap'
tgt_catalog = 'gold'

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

# Configuration
SILVER_SCHEMA = f"`{WORKSPACE_NAME}`.lh_bnj_silver.{src_catalog}"
GOLD_SCHEMA = f"`{WORKSPACE_NAME}`.lh_bnj_gold.{tgt_catalog}"  # Target schema
TABLE_NAME = "agg_ar_ap"

# Full table path
TARGET_TABLE = f"{GOLD_SCHEMA}.{TABLE_NAME}"

print(f"Source: {SILVER_SCHEMA}")
print(f"Target: {GOLD_SCHEMA}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_agg_ar_ap():
    # =====================================================
    # Step 2: Read Source Data from Silver Layer
    # =====================================================
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
        FROM `{WORKSPACE_NAME}`.lh_bnj_silver.xero.silver_invoices
        WHERE status IN ('AUTHORISED', 'PAID')
        AND invoice_date IS NOT NULL
    """)

    # Try to read payer_key from fact_payments joined with dim_payer
    try:
        df_payer_mapping = spark.sql(f"""
            SELECT DISTINCT
                fp.invoice_id,
                fp.payer_key,
                p.payer_type
            FROM {GOLD_SCHEMA}.fact_payment fp
            LEFT JOIN {GOLD_SCHEMA}.dim_payer p ON fp.payer_key = p.payer_id
        """)
        HAS_PAYER_DATA = True
        print(f"\nPayer mapping available: {df_payer_mapping.count()} invoice-payer records")
        print(f"Corporate (Insurance) invoices: {df_payer_mapping.filter(col('payer_type') == 'Insurance').count()}")
    except Exception as e:
        HAS_PAYER_DATA = False
        df_payer_mapping = None

    # =====================================================
    #Step 3: Calculate Aging Buckets and Metrics
    # =====================================================


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


    # =====================================================
    # Step 4: Combine AR and AP into Final Table
    # =====================================================

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
    df_agg_ar_ap = df_ar_final.union(df_ap_final)


    return df_agg_ar_ap



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# If you already defined this earlier in notebook, reuse it.
def enforce_to_table_schema_try_cast(df, target_table: str):
    """
    Force df to match target table schema using try_cast.
    Handles VOID (NullType) columns by always outputting NULL for them.
    """
    target_schema = spark.table(target_table).schema
    exprs = []

    for f in target_schema.fields:
        name = f.name
        sql_type = f.dataType.simpleString().lower()

        if sql_type == "void":
            exprs.append(F.lit(None).alias(name))
        elif name in df.columns:
            exprs.append(F.expr(f"try_cast(`{name}` as {sql_type})").alias(name))
        else:
            exprs.append(F.expr(f"cast(null as {sql_type})").alias(name))

    return df.select(*exprs)


try:
    df_raw = create_agg_ar_ap()

    # counts (keep same meaning as your previous code)
    src_cnt = tgt_cnt = df_raw.count()

    # Ensure table exists (NOTE: without schema this only registers; first run we will create from DF)
    if not spark.catalog.tableExists(TARGET_TABLE):
        # First load: create table with schema from df_raw
        df_raw.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(TARGET_TABLE)
    else:
        # Existing table: enforce schema tolerance (try_cast) then overwrite
        df = enforce_to_table_schema_try_cast(df_raw, TARGET_TABLE)

        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "false") \
            .saveAsTable(TARGET_TABLE)

    end_job_instance(
        batch_id, job_id, "SUCCESS",
        msg="Created agg_ar_ap (overwrite, schema-tolerant)",
        src_row_num=src_cnt, tgt_row_num=tgt_cnt
    )
    print("Successfully write table", TARGET_TABLE)
    print("OK")

except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed agg_ar_ap. {safe_exception_text(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # LEGACY LOGIC

# CELL ********************

def create_agg_ar_ap():
    """
    Create agg_ar_ap aggregate table for AR/AP Health dashboard.
    Combines AR aging (from fact_ar_aging) and AP aging (from fact_ap_aging).
    Aging buckets: 0-30, 31-60, 61-90, 91-180, 180+ days
    """
    
    # Read fact tables
    fact_ar = spark.table(GOLD_FACTS["ar_aging"])
    fact_ap = spark.table(GOLD_FACTS["ap_aging"])
    
    # AR Aging aggregation with 5 buckets
    ar_agg = fact_ar.withColumn(
        "aging_bucket_5",
        when(col("days_outstanding") <= 30, "0-30")
        .when(col("days_outstanding") <= 60, "31-60")
        .when(col("days_outstanding") <= 90, "61-90")
        .when(col("days_outstanding") <= 180, "91-180")
        .otherwise("180+")
    ).groupBy("date_key", "invoice_id").agg(
        # AR bucket amounts and counts
        sum(when(col("aging_bucket_5") == "0-30", col("outstanding_amount")).otherwise(0)).cast(DecimalType(18,2)).alias("ar_0_30_days_amount"),
        sum(when(col("aging_bucket_5") == "0-30", 1).otherwise(0)).alias("ar_0_30_days_count"),
        sum(when(col("aging_bucket_5") == "31-60", col("outstanding_amount")).otherwise(0)).cast(DecimalType(18,2)).alias("ar_31_60_days_amount"),
        sum(when(col("aging_bucket_5") == "31-60", 1).otherwise(0)).alias("ar_31_60_days_count"),
        sum(when(col("aging_bucket_5") == "61-90", col("outstanding_amount")).otherwise(0)).cast(DecimalType(18,2)).alias("ar_61_90_days_amount"),
        sum(when(col("aging_bucket_5") == "61-90", 1).otherwise(0)).alias("ar_61_90_days_count"),
        sum(when(col("aging_bucket_5") == "91-180", col("outstanding_amount")).otherwise(0)).cast(DecimalType(18,2)).alias("ar_91_180_days_amount"),
        sum(when(col("aging_bucket_5") == "91-180", 1).otherwise(0)).alias("ar_91_180_days_count"),
        sum(when(col("aging_bucket_5") == "180+", col("outstanding_amount")).otherwise(0)).cast(DecimalType(18,2)).alias("ar_over_180_days_amount"),
        sum(when(col("aging_bucket_5") == "180+", 1).otherwise(0)).alias("ar_over_180_days_count"),
        sum("outstanding_amount").cast(DecimalType(18,2)).alias("total_ar_amount"),
        avg("days_outstanding").cast(FloatType()).alias("ar_days_outstanding"),
        first("payer_key").alias("payer_key"),
        first("patient_key").alias("patient_key")
    )
    
    # AP Aging aggregation with 5 buckets
    ap_agg = fact_ap.withColumn(
        "aging_bucket_5",
        when(col("days_outstanding") <= 30, "0-30")
        .when(col("days_outstanding") <= 60, "31-60")
        .when(col("days_outstanding") <= 90, "61-90")
        .when(col("days_outstanding") <= 180, "91-180")
        .otherwise("180+")
    ).groupBy("date_key").agg(
        # AP bucket amounts and counts
        sum(when(col("aging_bucket_5") == "0-30", col("outstanding_amount")).otherwise(0)).cast(FloatType()).alias("ap_0_30_days_amount"),
        sum(when(col("aging_bucket_5") == "0-30", 1).otherwise(0)).cast(IntegerType()).alias("ap_0_30_days_count"),
        sum(when(col("aging_bucket_5") == "31-60", col("outstanding_amount")).otherwise(0)).cast(FloatType()).alias("ap_31_60_days_amount"),
        sum(when(col("aging_bucket_5") == "31-60", 1).otherwise(0)).cast(IntegerType()).alias("ap_31_60_days_count"),
        sum(when(col("aging_bucket_5") == "61-90", col("outstanding_amount")).otherwise(0)).cast(FloatType()).alias("ap_61_90_days_amount"),
        sum(when(col("aging_bucket_5") == "61-90", 1).otherwise(0)).cast(IntegerType()).alias("ap_61_90_days_count"),
        sum(when(col("aging_bucket_5") == "91-180", col("outstanding_amount")).otherwise(0)).cast(FloatType()).alias("ap_91_180_days_amount"),
        sum(when(col("aging_bucket_5") == "91-180", 1).otherwise(0)).cast(IntegerType()).alias("ap_91_180_days_count"),
        sum(when(col("aging_bucket_5") == "180+", col("outstanding_amount")).otherwise(0)).cast(FloatType()).alias("ap_over_180_days_amount"),
        sum(when(col("aging_bucket_5") == "180+", 1).otherwise(0)).cast(IntegerType()).alias("ap_over_180_days_count"),
        sum("outstanding_amount").cast(FloatType()).alias("total_ap_amount"),
        avg("days_outstanding").cast(FloatType()).alias("ap_days_outstanding"),
        # Payment timing counts (based on is_overdue)
        sum(when(col("is_overdue") == False, 1).otherwise(0)).cast(IntegerType()).alias("ap_paid_on_time_count"),
        sum(when(col("is_overdue") == True, 1).otherwise(0)).cast(IntegerType()).alias("ap_paid_late_count"),
        lit(0).cast(IntegerType()).alias("ap_paid_early_count")  # Placeholder - would need payment date vs due date
    )
    
    # Join AR and AP aggregations
    agg_ar_ap = ar_agg.join(ap_agg, "date_key", "outer")
    
    # Calculate health scores (0-100 scale, higher is better)
    # AR Score: Penalize older buckets more heavily
    agg_ar_ap = agg_ar_ap.withColumn(
        "ar_aging_score",
        (
            lit(100) - 
            (coalesce(col("ar_31_60_days_amount"), lit(0)) / greatest(col("total_ar_amount"), lit(1)) * 10) -
            (coalesce(col("ar_61_90_days_amount"), lit(0)) / greatest(col("total_ar_amount"), lit(1)) * 20) -
            (coalesce(col("ar_91_180_days_amount"), lit(0)) / greatest(col("total_ar_amount"), lit(1)) * 30) -
            (coalesce(col("ar_over_180_days_amount"), lit(0)) / greatest(col("total_ar_amount"), lit(1)) * 40)
        ).cast(FloatType())
    ).withColumn(
        "ap_aging_score",
        (
            lit(100) - 
            (coalesce(col("ap_31_60_days_amount"), lit(0)) / greatest(col("total_ap_amount"), lit(1)) * 10) -
            (coalesce(col("ap_61_90_days_amount"), lit(0)) / greatest(col("total_ap_amount"), lit(1)) * 20) -
            (coalesce(col("ap_91_180_days_amount"), lit(0)) / greatest(col("total_ap_amount"), lit(1)) * 30) -
            (coalesce(col("ap_over_180_days_amount"), lit(0)) / greatest(col("total_ap_amount"), lit(1)) * 40)
        ).cast(FloatType())
    ).withColumn(
        "ar_ap_health_score",
        ((coalesce(col("ar_aging_score"), lit(50)) + coalesce(col("ap_aging_score"), lit(50))) / 2).cast(FloatType())
    )
    
    # Add metadata columns
    agg_ar_ap = agg_ar_ap.withColumn("report_date", current_date()) \
        .withColumn("invoice_date_key", col("date_key")) \
        .withColumn("corporate_id", lit(None).cast(StringType())) \
        .withColumn("location_key", lit(1).cast(LongType())) \
        .withColumn("inventory_key", lit(-1).cast(LongType())) \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # Select final columns matching target schema
    result = agg_ar_ap.select(
        col("ap_0_30_days_amount").cast(FloatType()),
        col("ap_0_30_days_count").cast(IntegerType()),
        col("ap_31_60_days_amount").cast(FloatType()),
        col("ap_31_60_days_count").cast(IntegerType()),
        col("ap_61_90_days_amount").cast(FloatType()),
        col("ap_61_90_days_count").cast(IntegerType()),
        col("ap_91_180_days_amount").cast(FloatType()),
        col("ap_91_180_days_count").cast(IntegerType()),
        col("ap_aging_score").cast(FloatType()),
        col("ap_days_outstanding").cast(FloatType()),
        col("ap_over_180_days_amount").cast(FloatType()),
        col("ap_over_180_days_count").cast(IntegerType()),
        col("ap_paid_early_count").cast(IntegerType()),
        col("ap_paid_late_count").cast(IntegerType()),
        col("ap_paid_on_time_count").cast(IntegerType()),
        col("ar_0_30_days_amount").cast(DecimalType(18,2)),
        col("ar_0_30_days_count").cast(LongType()),
        col("ar_31_60_days_amount").cast(DecimalType(18,2)),
        col("ar_31_60_days_count").cast(LongType()),
        col("ar_61_90_days_amount").cast(DecimalType(18,2)),
        col("ar_61_90_days_count").cast(LongType()),
        col("ar_91_180_days_amount").cast(DecimalType(18,2)),
        col("ar_91_180_days_count").cast(LongType()),
        col("ar_aging_score").cast(FloatType()),
        col("ar_ap_health_score").cast(FloatType()),
        col("ar_days_outstanding").cast(FloatType()),
        col("ar_over_180_days_amount").cast(DecimalType(18,2)),
        col("ar_over_180_days_count").cast(LongType()),
        col("corporate_id").cast(StringType()),
        col("date_key").cast(IntegerType()),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("inventory_key").cast(LongType()),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("location_key").cast(LongType()),
        col("report_date").cast(DateType()),
        col("total_ap_amount").cast(FloatType()),
        col("total_ar_amount").cast(DecimalType(18,2))
    )
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
