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

# MARKDOWN ********************

# # Gold Layer ETL Transformations - Part 2
# ## Aggregate Tables & Health Score Calculations
# 
# **Purpose**: Create pre-aggregated tables with health score calculations for CFO Dashboard
# 
# **Dependencies**: Requires Part 1 (dimensions and facts) to be completed first
# 
# **Health Score Formulas**:
# - **Financial Health**: 50% P&L + 25% AR/AP + 25% Cashflow
# - **Operational Health**: 40% Claims + 20% Patient + 40% Inventory
# - **Overall Health**: Average of Financial and Operational

# MARKDOWN ********************

# ## Table of Contents
# 1. [Setup](#setup)
# 2. [AR/AP Aggregates](#ar-ap)
# 3. [P&L Aggregates](#pl)
# 4. [Cashflow Aggregates](#cashflow)
# 5. [Inventory Aggregates](#inventory)
# 6. [Claims Aggregates](#claims)
# 7. [Patient Aggregates](#patient)
# 8. [Health Score Composites](#health-scores)

# MARKDOWN ********************

# ## 1. Setup <a id='setup'></a>

# CELL ********************

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

GOLD_SCHEMA = "gold"

# Health score calculation functions

def calculate_ar_aging_score(df, ar_weights={'30-45': 1.0, '46-90': 0.7, '>90': 0.0}):
    """
    Calculate AR aging score based on aging buckets
    Formula: Weighted average of amounts in each bucket
    """
    ar_amt_score = (
        (col('ar_0_30_days_amount') * ar_weights['0-30'] +
         col('ar_31_60_days_amount') * ar_weights['31-60'] +
         col('ar_61_90_days_amount') * ar_weights['61-90'] +
         col('ar_over_90_days_amount') * ar_weights['>90']) /
        nullif(col('total_ar_amount'), 0) * 100
    )
    
    ar_cnt_score = (
        (col('ar_0_30_days_count') * ar_weights['0-30'] +
         col('ar_31_60_days_count') * ar_weights['31-60'] +
         col('ar_61_90_days_count') * ar_weights['61-90'] +
         col('ar_over_90_days_count') * ar_weights['>90']) /
        nullif((col('ar_0_30_days_count') + col('ar_31_60_days_count') + 
                col('ar_61_90_days_count') + col('ar_over_90_days_count')), 0) * 100
    )
    
    # Combined: 70% amount, 30% count
    return (ar_amt_score * 0.7 + ar_cnt_score * 0.3)

def calculate_ap_aging_score(df, ap_weights={'0-30': 0.3, '31-60': 0.6, '61-90': 0.9, '>90': 1.0}):
    """
    Calculate AP aging score based on aging buckets
    Note: Longer AP aging is BETTER for cashflow
    """
    ap_amt_score = (
        (col('ap_0_30_days_amount') * ap_weights['0-30'] +
         col('ap_31_60_days_amount') * ap_weights['31-60'] +
         col('ap_61_90_days_amount') * ap_weights['61-90'] +
         col('ap_over_90_days_amount') * ap_weights['>90']) /
        nullif(col('total_ap_amount'), 0) * 100
    )
    
    ap_cnt_score = (
        (col('ap_0_30_days_count') * ap_weights['0-30'] +
         col('ap_31_60_days_count') * ap_weights['31-60'] +
         col('ap_61_90_days_count') * ap_weights['61-90'] +
         col('ap_over_90_days_count') * ap_weights['>90']) /
        nullif((col('ap_0_30_days_count') + col('ap_31_60_days_count') + 
                col('ap_61_90_days_count') + col('ap_over_90_days_count')), 0) * 100
    )
    
    # Combined: 70% amount, 30% count
    return (ap_amt_score * 0.7 + ap_cnt_score * 0.3)

def calculate_pl_component_score(actual, target):
    """
    Calculate P&L component score
    If actual <= target: proportional score
    If actual > target: 100 + bonus (capped at +50)
    """
    return when(actual <= target, 
                (actual / nullif(target, 0) * 100))\
           .otherwise(
                least(100 + ((actual - target) / nullif(target, 0) * 50), 150)
           )

print("✅ Health score calculation functions loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. AR/AP Aggregates <a id='ar-ap'></a>

# CELL ********************

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

GOLD_SCHEMA = "gold"

# ============================================================================
# CORRECTED: Health Score Calculation Functions with 3-bucket aging
# ============================================================================

def calculate_ar_aging_score_expr():
    """AR aging score — corrected PySpark-safe version"""
    
    # nullif(total_ar_amount, 0)
    denom_amt = when(col("total_ar_amount") == 0, lit(None)).otherwise(col("total_ar_amount"))
    
    # amount-based score
    ar_amt_score = (
        (
            coalesce(col('ar_30_45_days_amount'), lit(0)) * 1.0 +
            coalesce(col('ar_46_90_days_amount'), lit(0)) * 0.5 +
            coalesce(col('ar_over_90_days_amount'), lit(0)) * 0.0
        ) / denom_amt * 100
    )

    # total_count for nullif
    total_count = (
        coalesce(col('ar_30_45_days_count'), lit(0)) +
        coalesce(col('ar_46_90_days_count'), lit(0)) +
        coalesce(col('ar_over_90_days_count'), lit(0))
    )

    denom_cnt = when(total_count == 0, lit(None)).otherwise(total_count)

    # count-based score
    ar_cnt_score = (
        (
            coalesce(col('ar_30_45_days_count'), lit(0)) * 1.0 +
            coalesce(col('ar_46_90_days_count'), lit(0)) * 0.5 +
            coalesce(col('ar_over_90_days_count'), lit(0)) * 0.0
        ) / denom_cnt * 100
    )

    return (
        when(col('total_ar_amount') == 0, lit(100))  # handle zero AR
        .otherwise(ar_amt_score * 0.7 + ar_cnt_score * 0.3)
        .cast("decimal(5,2)")
    )


def calculate_ap_aging_score_expr():
    """AP aging score — corrected PySpark-safe version"""

    denom_amt = when(col("total_ap_amount") == 0, lit(None)).otherwise(col("total_ap_amount"))

    ap_amt_score = (
        (
            coalesce(col('ap_30_45_days_amount'), lit(0)) * 0.3 +
            coalesce(col('ap_46_90_days_amount'), lit(0)) * 0.6 +
            coalesce(col('ap_over_90_days_amount'), lit(0)) * 1.0
        ) / denom_amt * 100
    )

    total_count = (
        coalesce(col('ap_30_45_days_count'), lit(0)) +
        coalesce(col('ap_46_90_days_count'), lit(0)) +
        coalesce(col('ap_over_90_days_count'), lit(0))
    )

    denom_cnt = when(total_count == 0, lit(None)).otherwise(total_count)

    ap_cnt_score = (
        (
            coalesce(col('ap_30_45_days_count'), lit(0)) * 0.3 +
            coalesce(col('ap_46_90_days_count'), lit(0)) * 0.6 +
            coalesce(col('ap_over_90_days_count'), lit(0)) * 1.0
        ) / denom_cnt * 100
    )

    return (
        when(col('total_ap_amount') == 0, lit(75))  # baseline when AP = 0
        .otherwise(ap_amt_score * 0.7 + ap_cnt_score * 0.3)
        .cast("decimal(5,2)")
    )


def calculate_pl_component_score(actual, target):

    denom = when(target == 0, lit(None)).otherwise(target)

    return (
        when(actual <= target,
            (actual / denom * 100)
        )
        .otherwise(
            least(100 + ((actual - target) / denom * 50), lit(150))
        )
        .cast("decimal(5,2)")
    )


# ============================================================================
# CORRECTED: ETL Function for AGG_AR_AP_DAILY
# ============================================================================

def calculate_agg_ar_ap_daily(report_date=None):
    """
    Calculate daily AR/AP aggregates with CORRECTED aging buckets: 30-45, 46-90, >90
    """
    if report_date is None:
        report_date = datetime.now().date()
    
    print(f"🔄 Processing AR/AP for date: {report_date}")
    
    # Read fact tables
    fact_invoice_header = spark.table(f"{GOLD_SCHEMA}.fact_invoice_header")
    fact_payment = spark.table(f"{GOLD_SCHEMA}.fact_payment")
    dim_date = spark.table(f"{GOLD_SCHEMA}.dim_date")
    
    # Get report date key
    report_date_row = dim_date.filter(col('full_date') == lit(report_date)).select('date_key').first()
    if not report_date_row:
        print(f"⚠️  Date {report_date} not found in dim_date")
        return None
    report_date_key = report_date_row[0]
    
    # ========================================================================
    # Calculate AR (Accounts Receivable) - CORRECTED BUCKETS
    # ========================================================================
    
    # Get unpaid/partially paid invoices
    ar_by_invoice = fact_invoice_header.alias('inv') \
        .filter(col('inv.is_void') == False) \
        .filter(col('inv.is_finalized') == True) \
        .filter(col('inv.invoice_date_key') <= report_date_key) \
        .join(
            fact_payment.filter(col('is_voided') == False)
                .filter(col('payment_date_key') <= report_date_key)
                .groupBy('invoice_fact_key')
                .agg(sum('payment_amount').alias('total_paid'))
                .alias('pay'),
            col('inv.invoice_fact_key') == col('pay.invoice_fact_key'),
            'left'
        ) \
        .withColumn('ar_amount', col('inv.total_amount') - coalesce(col('pay.total_paid'), lit(0))) \
        .filter(col('ar_amount') > 0) \
        .join(
            dim_date.alias('dt'), 
            col('inv.invoice_date_key') == col('dt.date_key'), 
            'inner'
        ) \
        .withColumn('days_outstanding', datediff(lit(report_date), col('dt.full_date'))) \
        .select(
            col('inv.location_key').alias('location_key'),
            col('inv.invoice_id').alias('invoice_id'),
            col('inv.invoice_date_key').alias('invoice_date_key'),
            col('ar_amount'),
            col('days_outstanding')
        )
    
    # CORRECTED: Categorize into 3 aging buckets (30-45, 46-90, >90)
    ar_aging = ar_by_invoice \
        .groupBy('location_key', 'invoice_id', 'invoice_date_key') \
        .agg(
            sum('ar_amount').alias('total_ar_amount'),
            
            # CORRECTED BUCKETS: Amount by aging bucket
            sum(when(
                (col('days_outstanding') >= 30) & (col('days_outstanding') <= 45), 
                col('ar_amount')
            ).otherwise(0)).alias('ar_30_45_days_amount'),
            
            sum(when(
                (col('days_outstanding') > 45) & (col('days_outstanding') <= 90), 
                col('ar_amount')
            ).otherwise(0)).alias('ar_46_90_days_amount'),
            
            sum(when(
                col('days_outstanding') > 90, 
                col('ar_amount')
            ).otherwise(0)).alias('ar_over_90_days_amount'),
            
            # CORRECTED BUCKETS: Count by aging bucket
            sum(when(
                (col('days_outstanding') >= 30) & (col('days_outstanding') <= 45), 
                lit(1)
            ).otherwise(lit(0))).alias('ar_30_45_days_count'),
            
            sum(when(
                (col('days_outstanding') > 45) & (col('days_outstanding') <= 90), 
                lit(1)
            ).otherwise(lit(0))).alias('ar_46_90_days_count'),
            
            sum(when(
                col('days_outstanding') > 90, 
                lit(1)
            ).otherwise(lit(0))).alias('ar_over_90_days_count'),
            
            # Average days outstanding
            avg('days_outstanding').alias('ar_days_outstanding')
        )
    
    # ========================================================================
    # Calculate AP (Accounts Payable) - PLACEHOLDER
    # Note: Would need actual vendor/supplier data
    # ========================================================================
    
    ap_aging = ar_aging.select(
        col('location_key').alias('ap_location_key'),
        col('invoice_id').alias('ap_invoice_id'),
        col('invoice_date_key').alias('ap_invoice_date_key'),
        lit(0.0).cast('decimal(18,2)').alias('total_ap_amount'),
        lit(0.0).cast('decimal(18,2)').alias('ap_30_45_days_amount'),
        lit(0.0).cast('decimal(18,2)').alias('ap_46_90_days_amount'),
        lit(0.0).cast('decimal(18,2)').alias('ap_over_90_days_amount'),
        lit(0).cast('int').alias('ap_30_45_days_count'),
        lit(0).cast('int').alias('ap_46_90_days_count'),
        lit(0).cast('int').alias('ap_over_90_days_count'),
        lit(0.0).cast('decimal(10,2)').alias('ap_days_outstanding'),
        lit(0).cast('int').alias('ap_paid_on_time_count'),
        lit(0).cast('int').alias('ap_paid_early_count'),
        lit(0).cast('int').alias('ap_paid_late_count')
    )
    
    # ========================================================================
    # Combine AR and AP with health scores
    # ========================================================================
    
    # Join AR and AP using qualified column names
    agg_ar_ap = ar_aging.join(
        ap_aging,
        (ar_aging['location_key'] == ap_aging['ap_location_key']) &
        (ar_aging['invoice_id'] == ap_aging['ap_invoice_id']) &
        (ar_aging['invoice_date_key'] == ap_aging['ap_invoice_date_key']),
        'left'
    )
    
    agg_ar_ap_final = agg_ar_ap.select(
        ar_aging['location_key'],
        ar_aging['invoice_id'],
        ar_aging['invoice_date_key'],

        # AR metrics - CORRECTED BUCKETS
        coalesce(ar_aging['total_ar_amount'], lit(0)).cast('decimal(18,2)').alias('total_ar_amount'),
        coalesce(ar_aging['ar_30_45_days_amount'], lit(0)).cast('decimal(18,2)').alias('ar_30_45_days_amount'),
        coalesce(ar_aging['ar_46_90_days_amount'], lit(0)).cast('decimal(18,2)').alias('ar_46_90_days_amount'),
        coalesce(ar_aging['ar_over_90_days_amount'], lit(0)).cast('decimal(18,2)').alias('ar_over_90_days_amount'),
        coalesce(ar_aging['ar_30_45_days_count'], lit(0)).cast('int').alias('ar_30_45_days_count'),
        coalesce(ar_aging['ar_46_90_days_count'], lit(0)).cast('int').alias('ar_46_90_days_count'),
        coalesce(ar_aging['ar_over_90_days_count'], lit(0)).cast('int').alias('ar_over_90_days_count'),
        coalesce(ar_aging['ar_days_outstanding'], lit(0)).cast('decimal(10,2)').alias('ar_days_outstanding'),
        
        # AP metrics - CORRECTED BUCKETS
        coalesce(ap_aging['total_ap_amount'], lit(0)).cast('decimal(18,2)').alias('total_ap_amount'),
        coalesce(ap_aging['ap_30_45_days_amount'], lit(0)).cast('decimal(18,2)').alias('ap_30_45_days_amount'),
        coalesce(ap_aging['ap_46_90_days_amount'], lit(0)).cast('decimal(18,2)').alias('ap_46_90_days_amount'),
        coalesce(ap_aging['ap_over_90_days_amount'], lit(0)).cast('decimal(18,2)').alias('ap_over_90_days_amount'),
        coalesce(ap_aging['ap_30_45_days_count'], lit(0)).cast('int').alias('ap_30_45_days_count'),
        coalesce(ap_aging['ap_46_90_days_count'], lit(0)).cast('int').alias('ap_46_90_days_count'),
        coalesce(ap_aging['ap_over_90_days_count'], lit(0)).cast('int').alias('ap_over_90_days_count'),
        coalesce(ap_aging['ap_days_outstanding'], lit(0)).cast('decimal(10,2)').alias('ap_days_outstanding'),
        coalesce(ap_aging['ap_paid_on_time_count'], lit(0)).cast('int').alias('ap_paid_on_time_count'),
        coalesce(ap_aging['ap_paid_early_count'], lit(0)).cast('int').alias('ap_paid_early_count'),
        coalesce(ap_aging['ap_paid_late_count'], lit(0)).cast('int').alias('ap_paid_late_count'),
        
        # Audit
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    
    # Calculate health scores using CORRECTED formulas
    agg_ar_ap_with_scores = agg_ar_ap_final \
        .withColumn('ar_aging_score', calculate_ar_aging_score_expr()) \
        .withColumn('ap_aging_score', calculate_ap_aging_score_expr()) \
        .withColumn('ar_ap_health_score', 
                   (col('ar_aging_score') * 0.7 + col('ap_aging_score') * 0.3).cast('decimal(5,2)'))
    
    # Display for review
    row_count = agg_ar_ap_with_scores.count()
    print(f"✅ Processed {row_count:,} invoice(s)")
    
    # Uncomment to write to table
    agg_ar_ap_with_scores.write \
        .mode('overwrite') \
        .format('delta') \
        .partitionBy('location_key') \
        .saveAsTable(f"{GOLD_SCHEMA}.agg_ar_ap")
    
    return agg_ar_ap_with_scores

# Run the calculation
agg_ar_ap_daily = calculate_agg_ar_ap_daily()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. P&L Aggregates <a id='pl'></a>

# CELL ********************

# ETL 12: AGG_PL_MONTHLY - Calculate monthly P&L
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


def calculate_pl_component_score(actual, target):
    """
    Calculate score for a P&L component
    Score: 100 (meets/exceeds target) to 0 (far below target)
    """
    return (
        when(target == 0, lit(100))
        .when(actual >= target, lit(100))
        .when(actual <= 0, lit(0))
        .otherwise((actual / target) * 100)
        .cast("decimal(5,2)")
    )


def calculate_agg_pl_monthly(year_month=None):
    """
    Calculate monthly P&L aggregates with health scores
    Formula weights: Revenue (50%), COGS (10%), GP (10%), OPEX (20%), Op Profit (10%)
    """

    if year_month is None:
        year_month = datetime.now().strftime("%Y-%m")

    year, month = year_month.split("-")
    year = int(year)
    month = int(month)

    # Load fact tables
    fact_invoice_header = spark.table(f"{GOLD_SCHEMA}.fact_invoice_header")
    fact_invoice_line = spark.table(f"{GOLD_SCHEMA}.fact_invoice_line")
    dim_date = spark.table(f"{GOLD_SCHEMA}.dim_date")

    # --------------------------
    # Revenue (Invoice Header)
    # --------------------------
    revenue = (
        fact_invoice_header
        .filter(col("is_void") == False)
        .filter(col("is_credit_note") == False)
        .groupBy(["location_key", "invoice_id", "invoice_date_key"])
        .agg(sum("total_amount").alias("total_revenue"))
    )

    # --------------------------
    # COGS (Invoice Line)
    # --------------------------
    cogs = (
        fact_invoice_line
        .join(
            fact_invoice_header.filter(col("is_void") == False),
            "invoice_fact_key",
            "inner"
        )
        .groupBy([
            fact_invoice_line.location_key,
            fact_invoice_line.invoice_id,
            fact_invoice_line.invoice_date_key,
            fact_invoice_line.inventory_key
        ])
        .agg(sum(fact_invoice_line.line_cost_total).alias("cost_of_sales"))
    )

    # ------------------------------------------------
    # Join Revenue + COGS, line-level output with inventory_key
    # ------------------------------------------------
    pl = (
        revenue.join(
            cogs,
            ["location_key", "invoice_id", "invoice_date_key"],
            "full_outer"
        )
        .select(
            col("location_key"),
            col("invoice_id"),
            col("invoice_date_key"),
            col("inventory_key").cast("string"),
            coalesce(col("total_revenue"), lit(0)).cast("decimal(18,2)").alias("total_revenue"),
            coalesce(col("cost_of_sales"), lit(0)).cast("decimal(18,2)").alias("cost_of_sales")
        )
    )

    # ------------------------------------------------
    # Add P&L Calculations
    # ------------------------------------------------
    agg_pl = pl.select(
        col("location_key"),
        col("invoice_id"),
        col("invoice_date_key"),
        col("inventory_key"),

        # Revenue
        col("total_revenue"),
        lit(None).cast("decimal(18,2)").alias("target_revenue"),
        (col("total_revenue") - lit(0)).cast("decimal(18,2)").alias("revenue_variance"),
        when(col("total_revenue") != 0,
             (col("total_revenue") / lit(100000)) * 100
        ).otherwise(lit(0)).cast("decimal(5,2)").alias("revenue_variance_pct"),

        # Cost of Sales
        col("cost_of_sales"),
        lit(None).cast("decimal(18,2)").alias("target_cogs"),
        (col("cost_of_sales") - lit(0)).cast("decimal(18,2)").alias("cogs_variance"),

        # Gross Profit
        (col("total_revenue") - col("cost_of_sales")).cast("decimal(18,2)").alias("gross_profit"),
        when(col("total_revenue") != 0,
             ((col("total_revenue") - col("cost_of_sales")) / col("total_revenue")) * 100
        ).otherwise(lit(0)).cast("decimal(5,2)").alias("gross_profit_margin"),
        lit(None).cast("decimal(18,2)").alias("target_gross_profit"),

        # Operating Expenses (dummy 20%)
        (col("total_revenue") * lit(0.20)).cast("decimal(18,2)").alias("operating_expenses"),
        lit(None).cast("decimal(18,2)").alias("target_opex"),
        lit(0).cast("decimal(18,2)").alias("opex_variance"),

        # Operating Profit
        (col("total_revenue") - col("cost_of_sales")).cast("decimal(18,2)").alias("operating_profit"),
        when(col("total_revenue") != 0,
             ((col("total_revenue") - col("cost_of_sales")) / col("total_revenue")) * 100
        ).otherwise(lit(0)).cast("decimal(5,2)").alias("operating_profit_margin"),
        lit(None).cast("decimal(18,2)").alias("target_operating_profit")
    )

    # ------------------------------------------------
    # Add Health Scores
    # ------------------------------------------------
    agg_pl_with_scores = (
        agg_pl
        .withColumn("revenue_score", calculate_pl_component_score(col("total_revenue"), lit(100000)))
        .withColumn("cogs_score", calculate_pl_component_score(lit(50000) - col("cost_of_sales"), lit(0)))
        .withColumn("gross_profit_score", calculate_pl_component_score(col("gross_profit"), lit(50000)))
        .withColumn("opex_score", calculate_pl_component_score(lit(30000) - col("operating_expenses"), lit(0)))
        .withColumn("operating_profit_score", calculate_pl_component_score(col("operating_profit"), lit(20000)))
        .withColumn(
            "pl_health_score",
            (
                col("revenue_score") * 0.5 +
                col("cogs_score") * 0.1 +
                col("gross_profit_score") * 0.1 +
                col("opex_score") * 0.2 +
                col("operating_profit_score") * 0.1
            ).cast("decimal(5,2)")
        )
        .withColumn("dw_created_at", current_timestamp())
        .withColumn("dw_updated_at", current_timestamp())
    )

    # ------------------------------------------------
    # Final Output
    # ------------------------------------------------
    final_df = agg_pl_with_scores.select(
        "location_key",
        "invoice_id",
        "invoice_date_key",
        "inventory_key",
        "total_revenue",
        "target_revenue",
        "revenue_variance",
        "revenue_variance_pct",
        "cost_of_sales",
        "target_cogs",
        "cogs_variance",
        "gross_profit",
        "gross_profit_margin",
        "target_gross_profit",
        "operating_expenses",
        "target_opex",
        "opex_variance",
        "operating_profit",
        "operating_profit_margin",
        "target_operating_profit",
        "revenue_score",
        "cogs_score",
        "gross_profit_score",
        "opex_score",
        "operating_profit_score",
        "pl_health_score",
        "dw_created_at",
        "dw_updated_at"
    )

    # ------------------------------------------------
    # WRITE TO DELTA
    # ------------------------------------------------
    final_df.write \
        .mode("overwrite") \
        .format("delta") \
        .partitionBy("location_key") \
        .saveAsTable(f"{GOLD_SCHEMA}.agg_pl")

    row_count = final_df.count()
    print(f"AGG_PL_MONTHLY created with {row_count:,} rows for {year_month}")

    return final_df


# Execute
agg_pl_monthly = calculate_agg_pl_monthly()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Health Score Composites <a id='health-scores'></a>

# CELL ********************

# ETL 13: AGG_FINANCIAL_HEALTH_MONTHLY - Calculate composite financial health

def calculate_agg_financial_health_monthly(year_month=None):
    """
    Calculate composite financial health score
    Formula: 50% P&L + 25% AR/AP + 25% Cashflow
    """
    # if year_month is None:
    #     year_month = datetime.now().strftime('%Y-%m')
    
    # year, month = year_month.split('-')
    # year = int(year)
    # month = int(month)
    
    # Read component scores
    agg_pl = spark.table(f"{GOLD_SCHEMA}.agg_pl") \
        # .filter((col('year') == year) & (col('month') == month))
    
    # # Get latest AR/AP score for the month
    # dim_date = spark.table(f"{GOLD_SCHEMA}.dim_date")
    # last_day_of_month = dim_date.filter(
    #     (col('year') == year) & (col('month') == month)
    # ).agg(max('date_key').alias('max_date_key')).first()['max_date_key']
    
    agg_ar_ap = spark.table(f"{GOLD_SCHEMA}.agg_ar_ap")
    
    # Placeholder for cashflow score (would be calculated similarly)
    cashflow_score = lit(75.0)  # Placeholder
    
    # Combine scores
    financial_health = agg_pl \
        .join(agg_ar_ap, ['location_key', 'invoice_id', 'invoice_date_key'], 'left') \
        .select(
            # lit(year_month).alias('year_month'),
            # lit(year).alias('year'),
            # lit(month).alias('month'),
            agg_pl.location_key,
            agg_pl.invoice_id,
            agg_pl.invoice_date_key,
            # Component scores
            col('pl_health_score'),
            coalesce(col('ar_ap_health_score'), lit(0)).alias('ar_ap_health_score'),
            cashflow_score.alias('cashflow_health_score'),
            
            # Composite Financial Health Score
            (col('pl_health_score') * 0.5 + 
             coalesce(col('ar_ap_health_score'), lit(0)) * 0.25 + 
             cashflow_score * 0.25).alias('financial_health_score'),
            
            # Audit
            current_timestamp().alias('dw_created_at'),
            current_timestamp().alias('dw_updated_at')
        )
    
    # Add traffic light status
    financial_health_final = financial_health.withColumn(
        'health_status',
        when(col('financial_health_score') >= 75, 'Green')
        .when(col('financial_health_score') >= 26, 'Yellow')
        .otherwise('Red')
    ).withColumn(
        'health_status_color',
        when(col('health_status') == 'Green', '#00FF00')
        .when(col('health_status') == 'Yellow', '#FFFF00')
        .otherwise('#FF0000')
    )
    
    # # Write to Delta table
    financial_health_final.write \
        .mode('append') \
        .format('delta') \
        .partitionBy('location_key') \
        .saveAsTable(f"{GOLD_SCHEMA}.agg_financial_health")
    
    row_count = financial_health_final.count()
    print(f"✅ AGG_FINANCIAL_HEALTH_MONTHLY created with {row_count:,} rows for {year_month}")
    return financial_health_final

# Execute
agg_financial_health = calculate_agg_financial_health_monthly()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ETL 14: AGG_OVERALL_HEALTH_MONTHLY - Calculate overall organizational health

def calculate_agg_overall_health_monthly(year_month=None):
    """
    Calculate overall organizational health score
    Formula: Average of Financial and Operational Health
    """
    # if year_month is None:
    #     year_month = datetime.now().strftime('%Y-%m')
    
    # year, month = year_month.split('-')
    # year = int(year)
    # month = int(month)
    
    # Read component scores
    agg_financial = spark.table(f"{GOLD_SCHEMA}.agg_financial_health")
        # .filter((col('year') == year) & (col('month') == month))
    
    # Placeholder for operational health (would be calculated from inventory, claims, patient)
    operational_score = lit(70.0)
    
    # Combine scores
    overall_health = agg_financial.select(
        # lit(year_month).alias('year_month'),
        # lit(year).alias('year'),
        # lit(month).alias('month'),
        col('location_key'),
        col('invoice_id'),
        col('invoice_date_key'),
        
        # Component scores
        col('financial_health_score'),
        operational_score.alias('operational_health_score'),
        
        # Overall Health Score
        ((col('financial_health_score') + operational_score) / 2).alias('overall_health_score'),
        
        # Year-end forecast (simple linear projection)
        ((col('financial_health_score') + operational_score) / 2).alias('forecasted_year_end_score'),
        lit(80.0).alias('target_year_end_score'),  # From config
        (((col('financial_health_score') + operational_score) / 2) / 80.0 * 100).alias('progress_to_target_pct'),
        
        # Audit
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    
    # Add traffic light status
    overall_health_final = overall_health.withColumn(
        'health_status',
        when(col('overall_health_score') >= 75, 'Green')
        .when(col('overall_health_score') >= 26, 'Yellow')
        .otherwise('Red')
    )
    # Write to Delta table
    overall_health_final.write \
        .mode('append') \
        .format('delta') \
        .partitionBy('location_key') \
        .saveAsTable(f"{GOLD_SCHEMA}.agg_overall_health")
    
    row_count = overall_health_final.count()
    print(f"✅ AGG_OVERALL_HEALTH_MONTHLY created with {row_count:,} rows for {year_month}")
    return overall_health_final

# Execute
agg_overall_health = calculate_agg_overall_health_monthly()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary
# 
# ### Completed ETL Jobs (Part 2):
# 
# **Aggregate Tables**:
# 1. ✅ `agg_ar_ap_daily` - Daily AR/AP with aging and health scores
# 2. ✅ `agg_pl_monthly` - Monthly P&L with targets and health scores
# 
# **Health Score Composites**:
# 1. ✅ `agg_financial_health_monthly` - Financial health composite (50% P&L + 25% AR/AP + 25% Cashflow)
# 2. ✅ `agg_overall_health_monthly` - Overall organizational health
# 
# ### Health Score Formulas Implemented:
# 
# **AR/AP Health**:
# - AR Aging Score: Weighted by aging buckets (0-30: 100%, 31-60: 70%, 61-90: 40%, >90: 0%)
# - AP Aging Score: Longer aging is better (0-30: 30%, 31-60: 60%, 61-90: 90%, >90: 100%)
# - Combined: 70% AR + 30% AP
# 
# **P&L Health**:
# - Revenue (50%), COGS (10%), GP (10%), OPEX (20%), Op Profit (10%)
# - Score formula: actual/target × 100 (with bonus for exceeding target)
# 
# **Financial Health**:
# - 50% P&L + 25% AR/AP + 25% Cashflow
# 
# **Overall Health**:
# - Average of Financial and Operational Health
# 
# ### Traffic Light Thresholds:
# - 🟢 Green: 75-100
# - 🟡 Yellow: 26-74
# - 🔴 Red: 0-25
# 
# ### Next Steps:
# 1. Complete remaining aggregate tables (inventory, claims, patient)
# 2. Implement operational health composite
# 3. Set up config table for targets
# 4. Create incremental load processes
# 5. Schedule daily/monthly refreshes

