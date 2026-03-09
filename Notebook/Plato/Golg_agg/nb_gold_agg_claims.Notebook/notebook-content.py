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

# # Gold Aggregation: `agg_claims` (Single Table)


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '2595'
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

GOLD_FACTS = {"claims": f"lh_bnj_gold.{tgt_catalog}.fact_claims"}
AGG_TABLES = {"claims": f"lh_bnj_gold.{tgt_catalog}.agg_claims"}
GOLD_DIMENSIONS = {"payer": f"lh_bnj_gold.{tgt_catalog}.dim_payer"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_agg_claims():
    """
    Create agg_claims aggregate table for Claims Health dashboard.
    Tracks claim submission, approval, rejection rates and turnaround.
    
    Note: PLATO doesn't have explicit claim status tracking.
    Using invoice status as proxy:
    - Submitted = all claims
    - Approved = is_finalized = True and not void
    - Rejected = is_void = True (voided claims)
    - Pending = is_finalized = False
    """
    
    # Read fact_claims
    fact_claims = spark.table(GOLD_FACTS["claims"])
    
    # Read dim_payer (corporates) for corporate_key
    dim_payer = spark.table(GOLD_DIMENSIONS["payer"]).select("payer_key", "payer_id")
    
    # Classify claims by status
    claims_with_status = fact_claims.withColumn(
        "claim_status",
        when(col("is_void") == True, "Rejected")
        .when(col("is_finalized") == True, "Approved")
        .otherwise("Pending")
    ).withColumn(
        # Turnaround days - days from claim_date to finalized/void
        # Since we don't have response date, use current_date for pending
        "turnaround_days",
        datediff(current_date(), col("claim_date")).cast(IntegerType())
    )
    
    # Aggregate by date and invoice
    agg_claims = claims_with_status.groupBy("date_key", "invoice_id", "location_key") \
        .agg(
            # Total claims
            count("*").alias("claims_submitted"),
            
            # By status
            count(when(col("claim_status") == "Approved", True)).alias("claims_approved"),
            count(when(col("claim_status") == "Rejected", True)).alias("claims_rejected"),
            count(when(col("claim_status") == "Pending", True)).alias("claims_pending"),
            
            # Turnaround metrics
            avg("turnaround_days").cast(FloatType()).alias("avg_turnaround_days"),
            max("turnaround_days").cast(IntegerType()).alias("max_turnaround_days"),
            expr("percentile_approx(turnaround_days, 0.5)").cast(FloatType()).alias("median_turnaround_days"),
            
            # Outstanding claims
            count(when(col("claim_status") == "Pending", True)).alias("total_claims_outstanding"),
            
            # Get first payer_key for the group
            first("payer_key").alias("corporate_key")
        )
    
    # Calculate percentages
    agg_claims = agg_claims.withColumn(
        "claims_submitted_pct", lit(100).cast(FloatType())
    ).withColumn(
        "claims_approved_pct",
        (col("claims_approved") / greatest(col("claims_submitted"), lit(1)) * 100).cast(FloatType())
    ).withColumn(
        "claims_rejected_pct",
        (col("claims_rejected") / greatest(col("claims_submitted"), lit(1)) * 100).cast(FloatType())
    ).withColumn(
        "claims_pending_pct",
        (col("claims_pending") / greatest(col("claims_submitted"), lit(1)) * 100).cast(FloatType())
    )
    
    # Calculate health scores (0-100)
    agg_claims = agg_claims.withColumn(
        "approval_score",
        col("claims_approved_pct").cast(FloatType())  # Higher approval rate = better
    ).withColumn(
        "rejection_score",
        (lit(100) - col("claims_rejected_pct")).cast(FloatType())  # Lower rejection = better
    ).withColumn(
        "pending_score",
        (lit(100) - least(col("claims_pending_pct") * 2, lit(100))).cast(FloatType())  # Lower pending = better
    ).withColumn(
        "claims_health_score",
        (
            col("approval_score") * 0.5 +
            col("rejection_score") * 0.3 +
            col("pending_score") * 0.2
        ).cast(FloatType())
    )
    
    # Add metadata
    agg_claims = agg_claims \
        .withColumn("invoice_date_key", col("date_key")) \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # Select final columns matching target schema
    result = agg_claims.select(
        col("approval_score").cast(FloatType()),
        col("avg_turnaround_days").cast(FloatType()),
        col("claims_approved").cast(LongType()),
        col("claims_approved_pct").cast(FloatType()),
        col("claims_health_score").cast(FloatType()),
        col("claims_pending").cast(LongType()),
        col("claims_pending_pct").cast(FloatType()),
        col("claims_rejected").cast(LongType()),
        col("claims_rejected_pct").cast(FloatType()),
        col("claims_submitted").cast(LongType()),
        col("claims_submitted_pct").cast(FloatType()),
        col("corporate_key").cast(LongType()),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("location_key").cast(LongType()),
        col("max_turnaround_days").cast(IntegerType()),
        col("median_turnaround_days").cast(FloatType()),
        col("pending_score").cast(FloatType()),
        col("rejection_score").cast(FloatType()),
        col("total_claims_outstanding").cast(LongType())
    )
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    df = create_agg_claims()
    src_cnt = spark.table(GOLD_FACTS["claims"]).count()
    tgt_cnt = df.count()
    # log_data_quality("agg_claims", df, "claims_key")

    target_table = AGG_TABLES["claims"]
    key_cols = ["invoice_date_key", "invoice_id", "location_key"]

    # Ensure table exists before overwrite
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table}
    USING DELTA
    """)

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created agg_claims", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed agg_claims. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F

try:
    df = create_agg_claims()
    src_cnt = spark.table(GOLD_FACTS["claims"]).count()
    tgt_cnt = df.count()

    target_table = AGG_TABLES["claims"]
    key_cols = ["invoice_date_key", "invoice_id", "location_key"]

    # Optional but strongly recommended: validate merge key uniqueness in source df
    dup_cnt = (
        df.groupBy(*key_cols)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dup_cnt > 0:
        raise Exception(f"Merge key is not unique in source dataframe. Found {dup_cnt} duplicate key groups.")

    # Optional but strongly recommended: validate no nulls in key columns
    null_issues = []
    for c in key_cols:
        c_null_cnt = df.filter(F.col(c).isNull()).count()
        if c_null_cnt > 0:
            null_issues.append(f"{c}={c_null_cnt}")

    if null_issues:
        raise Exception(f"Merge key contains NULL values: {', '.join(null_issues)}")

    # If target table does not exist, do initial load
    if not spark.catalog.tableExists(target_table):
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)

    else:
        delta_target = DeltaTable.forName(spark, target_table)

        merge_condition = """
            t.invoice_date_key = s.invoice_date_key
            AND t.invoice_id = s.invoice_id
            AND t.location_key = s.location_key
        """

        (
            delta_target.alias("t")
            .merge(
                df.alias("s"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # Get final target count after merge
    final_tgt_cnt = spark.table(target_table).count()

    end_job_instance(
        batch_id,
        job_id,
        "SUCCESS",
        msg="Merged agg_claims",
        src_row_num=src_cnt,
        tgt_row_num=final_tgt_cnt
    )
    print("OK")

except Exception as e:
    end_job_instance(
        batch_id,
        job_id,
        "FAILED",
        msg=f"Failed agg_claims. {safe_exception_text(e)}"
    )
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
