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

# # Gold Aggregation: `agg_ar_ap` (Single Table)


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '5464'
src_catalog = "gold"
job_group_name = "gold_agg"
src_table = ""

GOLD_FACTS = {"ar_aging": "lh_bnj_gold.gold.fact_ar_aging", "ap_aging": "lh_bnj_gold.gold.fact_ap_aging"}
AGG_TABLES = {"ar_ap": "lh_bnj_gold.gold.agg_ar_ap"}

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
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_job_instance(batch_id, job_id, msg="Start agg_ar_ap")
try:
    df = create_agg_ar_ap()
    src_cnt = spark.table(GOLD_FACTS["ar_aging"]).count()
    tgt_cnt = df.count()
    # log_data_quality("agg_ar_ap", df, "ar_ap_key")

    target_table = AGG_TABLES["ar_ap"]

    # Ensure table exists before overwrite
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table}
    USING DELTA
    """)

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created agg_ar_ap", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed agg_ar_ap. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
