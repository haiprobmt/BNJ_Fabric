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
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Functions

# CELL ********************

def get_date_key(date_col):
    """Convert date column to date_key format (YYYYMMDD)"""
    return date_format(date_col, "yyyyMMdd").cast(IntegerType())

def lookup_dimension_key(df, dim_table, natural_key, surrogate_key, df_key):
    """Join fact with dimension to get surrogate key"""
    dim_df = spark.table(dim_table).select(col(natural_key), col(surrogate_key))
    return df.join(dim_df, df[df_key] == dim_df[natural_key], "left") \
             .drop(natural_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

GOLD_SCHEMA = "lh_bnj_gold.gold_test"
SILVER_SCHEMA = "lh_bnj_silver.xero_test"

SILVER_TABLES = {
    "xero_invoices": f"{SILVER_SCHEMA}.silver_xero_invoices"
}

GOLD_DIMENSIONS = {
    "date": f"{GOLD_SCHEMA}.dim_date",
    "patient": f"{GOLD_SCHEMA}.dim_patient",
    "location": f"{GOLD_SCHEMA}.dim_location",
    "product": f"{GOLD_SCHEMA}.dim_product",
    "supplier": f"{GOLD_SCHEMA}.dim_supplier",
    "payer": f"{GOLD_SCHEMA}.dim_payer",
    "account": f"{GOLD_SCHEMA}.dim_account"
}

GOLD_FACTS = {
    "invoice": f"{GOLD_SCHEMA}.fact_invoice",
    "payment": f"{GOLD_SCHEMA}.fact_payment",
    "claims": f"{GOLD_SCHEMA}.fact_claims",
    "appointment": f"{GOLD_SCHEMA}.fact_appointment",
    "inventory": f"{GOLD_SCHEMA}.fact_inventory",
    "inventory_snapshot": f"{GOLD_SCHEMA}.fact_inventory_snapshot",
    "ar_aging": f"{GOLD_SCHEMA}.fact_ar_aging",
    "ap_aging": f"{GOLD_SCHEMA}.fact_ap_aging",
    "cashflow": f"{GOLD_SCHEMA}.fact_cashflow",
    "pnl": f"{GOLD_SCHEMA}.fact_pnl"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Aggregate Tables Configuration

# CELL ********************

# Aggregate table names
AGG_TABLES = {
    "ar_ap": f"{GOLD_SCHEMA}.agg_ar_ap",
    "cashflow": f"{GOLD_SCHEMA}.agg_cashflow",
    "claims": f"{GOLD_SCHEMA}.agg_claims",
    "inventory": f"{GOLD_SCHEMA}.agg_inventory",
    "patient": f"{GOLD_SCHEMA}.agg_patient",
    "pl": f"{GOLD_SCHEMA}.agg_pl",
    "financial_health": f"{GOLD_SCHEMA}.agg_financial_health",
    "overall_health": f"{GOLD_SCHEMA}.agg_overall_health"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. agg_ar_ap - AR/AP Health Aggregate
# 
# Aggregates AR and AP aging data with health scores by aging bucket (0-30, 31-60, 61-90, 91-180, 180+ days).

# CELL ********************

from pyspark.sql.functions import (
    col, when, sum, count, avg, first, current_date, current_timestamp,
    lit, coalesce, greatest
)
from pyspark.sql.types import DecimalType, FloatType, IntegerType, LongType, StringType, DateType

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

# Create and save agg_ar_ap
try:
    agg_ar_ap_df = create_agg_ar_ap()
    agg_ar_ap_df.write.format("delta").mode("overwrite").saveAsTable(AGG_TABLES["ar_ap"])
    print(f"✅ Created {AGG_TABLES['ar_ap']} with {agg_ar_ap_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create agg_ar_ap: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. agg_cashflow - Cash Flow Health Aggregate
# 
# Calculates cash conversion cycle, DSO, DPO, DIO and operating cash flow metrics.

# CELL ********************

def create_agg_cashflow():
    """
    Create agg_cashflow aggregate table for Cash Flow Health dashboard.
    Calculates CCC (Cash Conversion Cycle), DSO, DPO, DIO metrics.
    
    CCC = DSO + DIO - DPO
    DSO = (AR / Revenue) * Days
    DIO = (Inventory / COGS) * Days
    DPO = (AP / Purchases) * Days
    """
    
    # Read fact tables
    fact_cashflow = spark.table(GOLD_FACTS["cashflow"])
    fact_invoice = spark.table(GOLD_FACTS["invoice"])
    fact_ar = spark.table(GOLD_FACTS["ar_aging"])
    fact_ap = spark.table(GOLD_FACTS["ap_aging"])
    fact_inventory = spark.table(GOLD_FACTS["inventory"])
    dim_date = spark.table(GOLD_DIMENSIONS["date"])
    
    # Get date range for calculations (use dim_date for month/year)
    date_info = dim_date.select("date_key", "year", "month_abbr").distinct()
    
    # Revenue by period (from invoices)
    revenue = fact_invoice.join(date_info, "date_key") \
        .groupBy("year", "month_abbr") \
        .agg(sum("net_amount").alias("revenue"))
    
    # AR totals
    ar_totals = fact_ar.groupBy("date_key") \
        .agg(sum("outstanding_amount").alias("total_ar_amount"))
    
    # AP totals  
    ap_totals = fact_ap.groupBy("date_key") \
        .agg(sum("outstanding_amount").alias("total_ap_amount"))
    
    # Operating cash flow (net cash in - cash out)
    cashflow_agg = fact_cashflow.groupBy("date_key") \
        .agg(
            sum("cash_in").alias("total_cash_in"),
            sum("cash_out").alias("total_cash_out"),
            sum("net_cashflow").cast(DecimalType(18,2)).alias("operating_cash_flow"),
            first("location_key").alias("location_key")
        )
    
    # Join with AR, AP
    agg_cashflow = cashflow_agg \
        .join(ar_totals, "date_key", "left") \
        .join(ap_totals, "date_key", "left")
    
    # Calculate metrics (assuming 30-day periods)
    days_in_period = 30
    
    agg_cashflow = agg_cashflow.withColumn(
        # DSO = (AR / Revenue) * Days - simplified using cash_in as proxy for revenue
        "days_sales_outstanding",
        (coalesce(col("total_ar_amount"), lit(0)) / greatest(col("total_cash_in"), lit(1)) * days_in_period).cast(DecimalType(18,2))
    ).withColumn(
        # DPO = (AP / Purchases) * Days - simplified using cash_out as proxy
        "days_payable_outstanding",
        (coalesce(col("total_ap_amount"), lit(0)) / greatest(col("total_cash_out"), lit(1)) * days_in_period).cast(FloatType())
    ).withColumn(
        # DIO placeholder - would need COGS and inventory data
        "days_inventory_outstanding",
        lit(30).cast(DecimalType(18,2))  # Default 30 days
    ).withColumn(
        # CCC = DSO + DIO - DPO
        "cash_conversion_cycle_days",
        (col("days_sales_outstanding") + col("days_inventory_outstanding") - col("days_payable_outstanding")).cast(FloatType())
    ).withColumn(
        # Operating Cash Flow Ratio = OCF / Current Liabilities
        "current_liabilities",
        coalesce(col("total_ap_amount"), lit(0)).cast(FloatType())
    ).withColumn(
        "operating_cash_flow_ratio",
        (col("operating_cash_flow") / greatest(col("current_liabilities"), lit(1))).cast(FloatType())
    )
    
    # Calculate component scores (0-100 scale)
    agg_cashflow = agg_cashflow.withColumn(
        "ar_score",
        greatest(lit(0), lit(100) - col("days_sales_outstanding")).cast(DecimalType(18,2))
    ).withColumn(
        "ap_score",
        least(lit(100), col("days_payable_outstanding") * 2).cast(FloatType())  # Higher DPO is better
    ).withColumn(
        "inventory_score",
        greatest(lit(0), lit(100) - col("days_inventory_outstanding")).cast(DecimalType(18,2))
    ).withColumn(
        "interest_expense", lit(0).cast(FloatType())
    ).withColumn(
        "interest_score", lit(100).cast(FloatType())
    ).withColumn(
        "principal_repayment", lit(0).cast(FloatType())
    ).withColumn(
        "principal_score", lit(100).cast(FloatType())
    ).withColumn(
        "total_debt_financing_expense", lit(0).cast(FloatType())
    )
    
    # Calculate overall cashflow health score
    agg_cashflow = agg_cashflow.withColumn(
        "cashflow_health_score",
        (
            (coalesce(col("ar_score"), lit(50)) * 0.3) +
            (coalesce(col("ap_score"), lit(50)) * 0.2) +
            (coalesce(col("inventory_score"), lit(50)) * 0.2) +
            (least(lit(100), greatest(lit(0), col("operating_cash_flow_ratio") * 50)) * 0.3)
        ).cast(FloatType())
    )
    
    # Add metadata
    agg_cashflow = agg_cashflow \
        .withColumn("invoice_date_key", col("date_key")) \
        .withColumn("invoice_id", lit(None).cast(StringType())) \
        .withColumn("inventory_id", lit(-1).cast(LongType())) \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # Select final columns matching target schema
    result = agg_cashflow.select(
        col("ap_score").cast(FloatType()),
        col("ar_score").cast(DecimalType(18,2)),
        col("cash_conversion_cycle_days").cast(FloatType()),
        col("cashflow_health_score").cast(FloatType()),
        col("current_liabilities").cast(FloatType()),
        col("days_inventory_outstanding").cast(DecimalType(18,2)),
        col("days_payable_outstanding").cast(FloatType()),
        col("days_sales_outstanding").cast(DecimalType(18,2)),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("interest_expense").cast(FloatType()),
        col("interest_score").cast(FloatType()),
        col("inventory_id").cast(LongType()),
        col("inventory_score").cast(DecimalType(18,2)),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("location_key").cast(LongType()),
        col("operating_cash_flow").cast(DecimalType(18,2)),
        col("operating_cash_flow_ratio").cast(FloatType()),
        col("principal_repayment").cast(FloatType()),
        col("principal_score").cast(FloatType()),
        col("total_ar_amount").cast(DecimalType(18,2)),
        col("total_debt_financing_expense").cast(FloatType())
    )
    
    return result

# Create and save agg_cashflow
try:
    agg_cashflow_df = create_agg_cashflow()
    agg_cashflow_df.write.format("delta").mode("overwrite").saveAsTable(AGG_TABLES["cashflow"])
    print(f"✅ Created {AGG_TABLES['cashflow']} with {agg_cashflow_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create agg_cashflow: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. agg_claims - Claims Health Aggregate
# 
# Tracks insurance claim submission, approval, rejection rates and turnaround times.

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

# Create and save agg_claims
try:
    agg_claims_df = create_agg_claims()
    agg_claims_df.write.format("delta").mode("overwrite").saveAsTable(AGG_TABLES["claims"])
    print(f"✅ Created {AGG_TABLES['claims']} with {agg_claims_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create agg_claims: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. agg_inventory - Inventory Health Aggregate
# 
# Tracks inventory turnover, expired stock, and days inventory outstanding.

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

# Create and save agg_inventory
try:
    agg_inventory_df = create_agg_inventory()
    agg_inventory_df.write.format("delta").mode("overwrite").saveAsTable(AGG_TABLES["inventory"])
    print(f"✅ Created {AGG_TABLES['inventory']} with {agg_inventory_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create agg_inventory: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. agg_patient - Patient Health Aggregate
# 
# Tracks patient retention, attrition, conversion rates and satisfaction metrics.

# CELL ********************

from pyspark.sql.functions import (
    col, when, count, countDistinct, sum, avg, first, current_timestamp,
    lit, coalesce, greatest, least
)
from pyspark.sql.types import IntegerType, LongType, FloatType, StringType
from datetime import datetime

def create_agg_patient():
    """
    Create agg_patient aggregate table for Patient Health dashboard.
    Tracks retention, attrition, conversion and satisfaction metrics.
    Overall aggregation across all time periods.
    """
    
    # Read fact_appointment for patient visit tracking
    fact_appointment = spark.table(GOLD_FACTS["appointment"])
    dim_date = spark.table(GOLD_DIMENSIONS["date"])
    dim_patient = spark.table(GOLD_DIMENSIONS["patient"])
    
    # Add date information
    appts_with_date = fact_appointment.join(
        dim_date.select(
            col("date_key"),
            "year", 
            "month", 
            "year_month"
        ),
        "date_key"
    )
    
    # Get overall patient counts (all time)
    overall_patients = appts_with_date.groupBy("patient_key", "date_key", "year", "month", "year_month").agg(
        count("*").alias("total_current_patients"),
        countDistinct(when(col("is_new_patient") == True, col("patient_key"))).alias("new_patients")
    ).withColumn("invoice_date_key", col("date_key"))
    
    # Calculate returning patients (total - new)
    overall_patients = overall_patients.withColumn(
        "returning_patients",
        (col("total_current_patients") - col("new_patients")).cast(IntegerType())
    )
    
    # Get patients with future appointments (proxy for retention)
    current_date_key = int(datetime.now().strftime("%Y%m%d"))
    future_appts = fact_appointment.filter(col("date_key") >= current_date_key) \
        .select("patient_key").distinct()
    patients_with_future = future_appts.count()
    
    # Calculate retention and attrition
    overall_patients = overall_patients.withColumn(
        "patients_with_future_appointments", lit(patients_with_future).cast(IntegerType())
    ).withColumn(
        "retention_rate_pct",
        (col("returning_patients") / greatest(col("total_current_patients"), lit(1)) * 100).cast(FloatType())
    ).withColumn(
        "attrition_rate_pct",
        (lit(100) - col("retention_rate_pct")).cast(FloatType())
    ).withColumn(
        "conversion_rate_pct",
        (col("returning_patients") / greatest(col("new_patients"), lit(1)) * 100).cast(FloatType())
    )
    
    # Follow-up metrics (average appointments per patient - overall)
    followup_stats = appts_with_date.agg(
        (count("*") / countDistinct("patient_key")).cast(FloatType()).alias("avg_followups_per_patient"),
        count("*").alias("total_followup_appointments")
    )
    
    overall_patients = overall_patients.crossJoin(followup_stats)
    
    # Year-over-year comparison (placeholder as we don't have prior period for overall)
    overall_patients = overall_patients.withColumn(
        "total_current_patients_yoy", lit(None).cast(LongType())
    )
    
    # Satisfaction metrics (placeholder - would need survey data)
    overall_patients = overall_patients.withColumn(
        "avg_satisfaction_score", lit(0).cast(IntegerType())
    ).withColumn(
        "total_surveys_completed", lit(0).cast(IntegerType())
    ).withColumn(
        "delighted_patients", lit(0).cast(IntegerType())
    ).withColumn(
        "delighted_patients_pct", lit(0).cast(IntegerType())
    ).withColumn(
        "unhappy_patients", lit(0).cast(IntegerType())
    ).withColumn(
        "unhappy_patients_pct", lit(0).cast(IntegerType())
    ).withColumn(
        "pending_patients", lit(0).cast(IntegerType())
    ).withColumn(
        "lost_patients", lit(0).cast(IntegerType())
    )
    
    # Calculate health scores
    overall_patients = overall_patients.withColumn(
        "retention_score", col("retention_rate_pct").cast(FloatType())
    ).withColumn(
        "attrition_score", (lit(100) - col("attrition_rate_pct")).cast(FloatType())
    ).withColumn(
        "conversion_score", least(col("conversion_rate_pct"), lit(100)).cast(FloatType())
    ).withColumn(
        "growth_score",
        when(col("new_patients") > 0, lit(80)).otherwise(lit(50)).cast(IntegerType())
    ).withColumn(
        "panel_score",
        least(lit(100), (col("total_current_patients") / lit(100) * 10)).cast(FloatType())
    ).withColumn(
        "delight_score", lit(50).cast(IntegerType())
    ).withColumn(
        "unhappy_score", lit(50).cast(IntegerType())
    ).withColumn(
        "patient_health_score",
        (
            col("retention_score") * 0.3 +
            col("conversion_score") * 0.2 +
            col("growth_score") * 0.2 +
            col("panel_score") * 0.3
        ).cast(FloatType())
    )
    
    # Add metadata
    overall_patients = overall_patients \
        .withColumn("invoice_id", lit(None).cast(StringType())) \
        .withColumn("location_key", lit(1).cast(IntegerType())) \
        .withColumn("patient_key", lit(-1).cast(LongType())) \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # Select final columns matching target schema
    result = overall_patients.select(
        col("attrition_rate_pct").cast(FloatType()),
        col("attrition_score").cast(FloatType()),
        col("avg_followups_per_patient").cast(FloatType()),
        col("avg_satisfaction_score").cast(IntegerType()),
        col("conversion_rate_pct").cast(FloatType()),
        col("conversion_score").cast(FloatType()),
        col("delight_score").cast(IntegerType()),
        col("delighted_patients").cast(IntegerType()),
        col("delighted_patients_pct").cast(IntegerType()),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("growth_score").cast(IntegerType()),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("location_key").cast(IntegerType()),
        col("lost_patients").cast(IntegerType()),
        col("month").cast(IntegerType()),
        col("new_patients").cast(IntegerType()),
        col("panel_score").cast(FloatType()),
        col("patient_health_score").cast(FloatType()),
        col("patient_key").cast(LongType()),
        col("patients_with_future_appointments").cast(IntegerType()),
        col("pending_patients").cast(IntegerType()),
        col("retention_rate_pct").cast(FloatType()),
        col("retention_score").cast(FloatType()),
        col("returning_patients").cast(IntegerType()),
        col("total_current_patients").cast(LongType()),
        col("total_current_patients_yoy").cast(LongType()),
        col("total_followup_appointments").cast(IntegerType()),
        col("total_surveys_completed").cast(IntegerType()),
        col("unhappy_patients").cast(IntegerType()),
        col("unhappy_patients_pct").cast(IntegerType()),
        col("unhappy_score").cast(IntegerType()),
        col("year").cast(IntegerType()),
        col("year_month").cast(StringType())
    )
    
    return result

# Create and save agg_patient
try:
    agg_patient_df = create_agg_patient()
    agg_patient_df.write.format("delta").mode("overwrite").saveAsTable(AGG_TABLES["patient"])
    print(f"✅ Created {AGG_TABLES['patient']} with {agg_patient_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create agg_patient: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. agg_financial_health - Financial Health Summary
# 
# Combines AR/AP, Cashflow, and P&L health scores into overall financial health.

# CELL ********************

def create_agg_financial_health():
    """
    Create agg_financial_health aggregate table.
    Combines AR/AP Health, Cashflow Health, and P&L Health into overall financial health.
    """
    
    # Read component aggregate tables
    try:
        agg_ar_ap = spark.table(AGG_TABLES["ar_ap"])
        agg_cashflow = spark.table(AGG_TABLES["cashflow"])
    except Exception as e:
        print(f"⚠️ Required aggregate tables not available: {e}")
        return None
    
    # Get AR/AP health scores
    ar_ap_scores = agg_ar_ap.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("ar_ap_health_score").cast(DecimalType(18,2)).alias("ar_ap_health_score"),
        col("location_key")
    )
    
    # Get Cashflow health scores
    cashflow_scores = agg_cashflow.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("cashflow_health_score")
    )
    
    # Join scores
    financial_health = ar_ap_scores.join(
        cashflow_scores, 
        ["invoice_date_key", "invoice_id"], 
        "outer"
    )
    
    # P&L Health Score placeholder (would need fact_pnl)
    financial_health = financial_health.withColumn(
        "pl_health_score",
        coalesce(col("cashflow_health_score"), lit(50)).cast(DecimalType(18,2))
    )
    
    # Calculate overall financial health score
    financial_health = financial_health.withColumn(
        "financial_health_score",
        (
            coalesce(col("ar_ap_health_score"), lit(50)) * 0.4 +
            coalesce(col("cashflow_health_score"), lit(50)) * 0.4 +
            coalesce(col("pl_health_score"), lit(50)) * 0.2
        ).cast(FloatType())
    )
    
    # Determine health status
    financial_health = financial_health.withColumn(
        "health_status",
        when(col("financial_health_score") >= 80, "Excellent")
        .when(col("financial_health_score") >= 60, "Good")
        .when(col("financial_health_score") >= 40, "Fair")
        .otherwise("Poor").cast(StringType())
    ).withColumn(
        "health_status_color",
        when(col("financial_health_score") >= 80, "green")
        .when(col("financial_health_score") >= 60, "blue")
        .when(col("financial_health_score") >= 40, "yellow")
        .otherwise("red").cast(StringType())
    )
    
    # Add metadata
    financial_health = financial_health \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # Select final columns matching target schema
    result = financial_health.select(
        col("ar_ap_health_score").cast(DecimalType(18,2)),
        col("cashflow_health_score").cast(FloatType()),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("financial_health_score").cast(FloatType()),
        col("health_status").cast(StringType()),
        col("health_status_color").cast(StringType()),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("location_key").cast(LongType()),
        col("pl_health_score").cast(DecimalType(18,2))
    )
    
    return result

# Create and save agg_financial_health
try:
    agg_financial_health_df = create_agg_financial_health()
    if agg_financial_health_df:
        agg_financial_health_df.write.format("delta").mode("overwrite").saveAsTable(AGG_TABLES["financial_health"])
        print(f"✅ Created {AGG_TABLES['financial_health']} with {agg_financial_health_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create agg_financial_health: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. agg_overall_health - Overall Health Score
# 
# Master health scorecard combining all health dimensions with YoY comparisons and targets.

# CELL ********************

def create_agg_overall_health():
    """
    Create agg_overall_health aggregate table.
    Master health scorecard combining all health dimensions.
    
    Overall Health Score = weighted average of:
    - Financial Health (30%)
    - Patient Health (20%)
    - Claims Health (15%)
    - Inventory Health (15%)
    - Operational Health (20%)
    """
    
    # Read all component aggregate tables
    try:
        agg_financial = spark.table(AGG_TABLES["financial_health"])
        agg_patient = spark.table(AGG_TABLES["patient"])
        agg_claims = spark.table(AGG_TABLES["claims"])
        agg_inventory = spark.table(AGG_TABLES["inventory"])
        agg_ar_ap = spark.table(AGG_TABLES["ar_ap"])
        agg_cashflow = spark.table(AGG_TABLES["cashflow"])
    except Exception as e:
        print(f"⚠️ Required aggregate tables not available: {e}")
        return None
    
    dim_date = spark.table(GOLD_DIMENSIONS["date"])
    
    # Get scores from each aggregate
    financial_scores = agg_financial.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("financial_health_score"),
        col("ar_ap_health_score").cast(FloatType()),
        col("cashflow_health_score"),
        col("pl_health_score").cast(DecimalType(18,2))
    )
    
    patient_scores = agg_patient.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("patient_health_score"),
        col("year"),
        col("month"),
        col("year_month")
    )
    
    claims_scores = agg_claims.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("claims_health_score")
    )
    
    inventory_scores = agg_inventory.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("inventory_health_score")
    )
    
    # Join all scores
    overall = financial_scores \
        .join(patient_scores, ["invoice_date_key", "invoice_id"], "outer") \
        .join(claims_scores, ["invoice_date_key", "invoice_id"], "outer") \
        .join(inventory_scores, ["invoice_date_key", "invoice_id"], "outer")
    
    # Count invoices
    invoice_counts = spark.table(GOLD_FACTS["invoice"]).groupBy("date_key") \
        .agg(count("*").alias("invoice_count"))
    
    overall = overall.join(
        invoice_counts, 
        overall["invoice_date_key"] == invoice_counts["date_key"], 
        "left"
    ).drop("date_key")
    
    # Operational Health = average of Claims + Inventory
    overall = overall.withColumn(
        "operational_health_score",
        (
            (coalesce(col("claims_health_score"), lit(50)) + 
             coalesce(col("inventory_health_score"), lit(50))) / 2
        ).cast(FloatType())
    )
    
    # Calculate Overall Health Score
    overall = overall.withColumn(
        "overall_health_score",
        (
            coalesce(col("financial_health_score"), lit(50)) * 0.30 +
            coalesce(col("patient_health_score"), lit(50)) * 0.20 +
            coalesce(col("claims_health_score"), lit(50)) * 0.15 +
            coalesce(col("inventory_health_score"), lit(50)) * 0.15 +
            coalesce(col("operational_health_score"), lit(50)) * 0.20
        ).cast(FloatType())
    )
    
    # YoY calculations
    window_yoy = Window.orderBy("year", "month")
    overall = overall.withColumn(
        "yoy_health_score_prior_year",
        lag("overall_health_score", 12).over(window_yoy)
    ).withColumn(
        "yoy_health_score",
        coalesce(col("yoy_health_score_prior_year"), col("overall_health_score")).cast(FloatType())
    ).withColumn(
        "yoy_health_score_change",
        (col("overall_health_score") - coalesce(col("yoy_health_score_prior_year"), col("overall_health_score"))).cast(FloatType())
    ).withColumn(
        "yoy_health_score_change_pct",
        (col("yoy_health_score_change") / greatest(col("yoy_health_score_prior_year"), lit(1)) * 100).cast(FloatType())
    ).withColumn(
        "overall_health_score_yoy",
        col("yoy_health_score_change").cast(FloatType())
    ).withColumn(
        "yoy_health_change",
        col("yoy_health_score_change").cast(FloatType())
    ).withColumn(
        "yoy_health_change_pct",
        col("yoy_health_score_change_pct").cast(FloatType())
    )
    
    # Targets and forecasting
    overall = overall.withColumn(
        "target_year_end_score", lit(80).cast(FloatType())
    ).withColumn(
        "progress_to_target_pct",
        (col("overall_health_score") / lit(80) * 100).cast(FloatType())
    ).withColumn(
        "forecasted_year_end_score",
        (col("overall_health_score") + coalesce(col("yoy_health_score_change"), lit(0)) * 0.5).cast(FloatType())
    )
    
    # Health status
    overall = overall.withColumn(
        "health_status",
        when(col("overall_health_score") >= 80, "Excellent")
        .when(col("overall_health_score") >= 60, "Good")
        .when(col("overall_health_score") >= 40, "Fair")
        .otherwise("Poor").cast(StringType())
    ).withColumn(
        "health_status_color",
        when(col("overall_health_score") >= 80, "green")
        .when(col("overall_health_score") >= 60, "blue")
        .when(col("overall_health_score") >= 40, "yellow")
        .otherwise("red").cast(StringType())
    )
    
    # Add metadata
    overall = overall \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # Select final columns matching target schema
    result = overall.select(
        col("ar_ap_health_score").cast(FloatType()),
        col("cashflow_health_score").cast(FloatType()),
        col("claims_health_score").cast(FloatType()),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("financial_health_score").cast(FloatType()),
        col("forecasted_year_end_score").cast(FloatType()),
        col("health_status").cast(StringType()),
        col("health_status_color").cast(StringType()),
        col("inventory_health_score").cast(FloatType()),
        col("invoice_count").cast(LongType()),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("month").cast(IntegerType()),
        col("operational_health_score").cast(FloatType()),
        col("overall_health_score").cast(FloatType()),
        col("overall_health_score_yoy").cast(FloatType()),
        col("patient_health_score").cast(FloatType()),
        col("pl_health_score").cast(DecimalType(18,2)),
        col("progress_to_target_pct").cast(FloatType()),
        col("target_year_end_score").cast(FloatType()),
        col("year").cast(IntegerType()),
        col("year_month").cast(StringType()),
        col("yoy_health_change").cast(FloatType()),
        col("yoy_health_change_pct").cast(FloatType()),
        col("yoy_health_score").cast(FloatType()),
        col("yoy_health_score_change").cast(FloatType()),
        col("yoy_health_score_change_pct").cast(FloatType()),
        col("yoy_health_score_prior_year").cast(FloatType())
    )
    
    return result

# Create and save agg_overall_health
try:
    agg_overall_health_df = create_agg_overall_health()
    if agg_overall_health_df:
        agg_overall_health_df.write.format("delta").mode("overwrite").saveAsTable(AGG_TABLES["overall_health"])
        print(f"✅ Created {AGG_TABLES['overall_health']} with {agg_overall_health_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create agg_overall_health: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. agg_pl - Profit & Loss Health Aggregate
# 
# Aggregates revenue, expenses, and profitability metrics with margin calculations and health scores.

# CELL ********************

# Re-import functions to avoid shadowing issues
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W

def create_agg_pl():
    """
    Create agg_pl aggregate table for Profit & Loss Health dashboard.
    Aggregates revenue, expenses, and profitability metrics at DAY level.
    SOURCE: XERO (official accounting book of record)
    """
    
    # Read XERO invoices (official book of record)
    xero_invoices = spark.table(SILVER_TABLES["xero_invoices"])
    
    # Revenue from XERO ACCREC (Accounts Receivable = Sales)
    xero_revenue = xero_invoices.filter(F.col("type") == "ACCREC") \
        .withColumn("date_key", F.date_format(F.col("invoice_date"), "yyyyMMdd").cast(IntegerType()))
    
    # Expenses from XERO ACCPAY (Accounts Payable = Bills/Purchases)
    xero_expenses = xero_invoices.filter(F.col("type") == "ACCPAY") \
        .withColumn("date_key", F.date_format(F.col("invoice_date"), "yyyyMMdd").cast(IntegerType()))
    
    # Revenue aggregation by DAY
    daily_revenue = xero_revenue.groupBy("date_key") \
        .agg(
            F.sum("total").cast(DecimalType(18,2)).alias("total_revenue"),
            F.sum("subtotal").cast(DecimalType(18,2)).alias("gross_revenue"),
            F.sum("total_tax").cast(DecimalType(18,2)).alias("tax_collected"),
            F.count("*").alias("invoice_count")
        )
    
    # Expenses aggregation by DAY
    daily_expenses = xero_expenses.groupBy("date_key") \
        .agg(
            F.sum("total").cast(DecimalType(18,2)).alias("cost_of_sales"),
            F.sum("subtotal").cast(DecimalType(18,2)).alias("operating_expenses_raw")
        )
    
    # Join revenue and expenses
    daily_pl = daily_revenue.join(daily_expenses, "date_key", "left") \
        .withColumn("cost_of_sales", F.coalesce(F.col("cost_of_sales"), F.lit(0)).cast(DecimalType(18,2))) \
        .withColumn("operating_expenses", (F.coalesce(F.col("operating_expenses_raw"), F.lit(0)) * 0.6).cast(DecimalType(18,2)))
    
    # Calculate profits and margins (avoid divide by zero)
    daily_pl = daily_pl.withColumn(
        "gross_profit",
        (F.col("total_revenue") - F.col("cost_of_sales")).cast(DecimalType(18,2))
    ).withColumn(
        "safe_revenue",
        F.when(F.col("total_revenue") > 0, F.col("total_revenue")).otherwise(F.lit(1))
    ).withColumn(
        "gross_profit_margin",
        (F.col("gross_profit") / F.col("safe_revenue") * 100).cast(FloatType())
    ).withColumn(
        "operating_profit",
        (F.col("gross_profit") - F.col("operating_expenses")).cast(DecimalType(18,2))
    ).withColumn(
        "operating_profit_margin",
        (F.col("operating_profit") / F.col("safe_revenue") * 100).cast(FloatType())
    )
    
    # Targets (using prior year same day as target * 1.10, or current if no prior)
    window_yoy = W.orderBy("date_key")
    daily_pl = daily_pl.withColumn(
        "revenue_prior_year",
        F.lag("total_revenue", 365).over(window_yoy)
    ).withColumn(
        "target_revenue",
        (F.coalesce(F.col("revenue_prior_year"), F.col("total_revenue")) * 1.10).cast(DecimalType(18,2))
    ).withColumn(
        "target_cogs",
        (F.col("target_revenue") * 0.40).cast(DecimalType(18,2))
    ).withColumn(
        "target_gross_profit",
        (F.col("target_revenue") - F.col("target_cogs")).cast(DecimalType(18,2))
    ).withColumn(
        "target_opex",
        (F.col("target_revenue") * 0.25).cast(DecimalType(18,2))
    ).withColumn(
        "target_operating_profit",
        (F.col("target_gross_profit") - F.col("target_opex")).cast(DecimalType(18,2))
    )
    
    # Variances and scores
    daily_pl = daily_pl.withColumn(
        "revenue_variance",
        (F.col("total_revenue") - F.col("target_revenue")).cast(DecimalType(18,2))
    ).withColumn(
        "revenue_variance_pct",
        (F.col("revenue_variance") / F.when(F.col("target_revenue")>0, F.col("target_revenue")).otherwise(F.lit(1)) * 100).cast(FloatType())
    )
    
    daily_pl = daily_pl.withColumn(
        "pl_health_score",
        (F.lit(50)).cast(FloatType())
    )
    
    # Add metadata
    daily_pl = daily_pl \
        .withColumn("invoice_date_key", F.col("date_key")) \
        .withColumn("invoice_id", F.lit(None).cast(StringType())) \
        .withColumn("location_key", F.lit(1).cast(LongType())) \
        .withColumn("inventory_key", F.lit(-1).cast(LongType())) \
        .withColumn("dw_created_at", F.current_timestamp()) \
        .withColumn("dw_updated_at", F.current_timestamp())
    
    # Select a compact set of columns
    result = daily_pl.select(
        F.col("cost_of_sales").cast(DecimalType(18,2)),
        F.col("dw_created_at"),
        F.col("dw_updated_at"),
        F.col("gross_profit").cast(DecimalType(18,2)),
        F.col("gross_profit_margin").cast(FloatType()),
        F.col("invoice_date_key").cast(IntegerType()),
        F.col("location_key").cast(LongType()),
        F.col("pl_health_score").cast(FloatType()),
        F.col("total_revenue").cast(DecimalType(18,2))
    )
    
    return result

# Create and save agg_pl
try:
    agg_pl_df = create_agg_pl()
    agg_pl_df.write.format("delta").mode("overwrite").saveAsTable(AGG_TABLES["pl"])
    print(f"✅ Created {AGG_TABLES['pl']} with {agg_pl_df.count()} rows (Source: XERO)")
except Exception as e:
    print(f"⚠️ Could not create agg_pl: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Aggregate Tables Summary

# CELL ********************

# Display summary of created aggregate tables
print("\n" + "="*60)
print("AGGREGATE TABLES CREATION SUMMARY")
print("="*60)

for agg_name, table_name in AGG_TABLES.items():
    try:
        count = spark.table(table_name).count()
        print(f"✅ {table_name}: {count:,} rows")
    except:
        print(f"⏳ {table_name}: Pending (waiting for source data)")

print("="*60)

# Show sample of overall health scores
print("\n" + "="*60)
print("OVERALL HEALTH SCORE SAMPLE")
print("="*60)
try:
    spark.table(AGG_TABLES["overall_health"]).select(
        "year_month",
        "overall_health_score",
        "financial_health_score",
        "patient_health_score",
        "claims_health_score",
        "inventory_health_score",
        "health_status"
    ).orderBy(desc("year_month")).show(6, truncate=False)
except Exception as e:
    print(f"Overall health not available: {e}")
print("="*60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
