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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold Layer ETL Transformations - Part 2 (Xero Data Source)
# ## Aggregate Tables & Health Score Calculations
# 
# **Purpose**: Create pre-aggregated tables with health score calculations for CFO Dashboard
# 
# **Data Source**: Xero accounting system via Gold layer tables
# 
# **Dependencies**: Requires Gold layer tables (fact_revenue, fact_expenses, fact_receivables, fact_payables)
# 
# **Health Score Formulas**:
# - **Financial Health**: 50% P&L + 25% AR/AP + 25% Cashflow
# - **Operational Health**: Set to NULL (no inventory/claims/patient data in Xero)
# - **Overall Health**: Based on Financial Health only
# 
# **Note**: Xero does not have inventory, claims, or patient visit data, so those sections produce NULL values to maintain schema compatibility with existing application.

# MARKDOWN ********************

# ## Table of Contents
# 1. [Setup](#setup)
# 2. [AR/AP Aggregates](#ar-ap)
# 3. [P&L Aggregates](#pl)
# 4. [Cashflow Aggregates](#cashflow)
# 5. [Inventory Aggregates](#inventory) - NULL (no Xero data)
# 6. [Claims Aggregates](#claims) - NULL (no Xero data)
# 7. [Patient Aggregates](#patient) - NULL (no Xero data)
# 8. [Health Score Composites](#health-scores)

# MARKDOWN ********************

# ## 1. Setup <a id='setup'></a>

# CELL ********************

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta

GOLD_SCHEMA = "gold"

# Health score calculation functions

def calculate_ar_aging_score(df, ar_weights={'0-30': 1.0, '31-60': 0.7, '61-90': 0.4, '91-180': 0.2, '>181': 0.0}):
    """
    Calculate AR aging score based on aging buckets
    Formula: Weighted average of amounts in each bucket
    Age groups: 0-30, 31-60, 61-90, 91-180, >181
    """
    # Calculate weighted amount
    ar_amt_weighted = (
        col('ar_0_30_days_amount') * ar_weights['0-30'] +
        col('ar_31_60_days_amount') * ar_weights['31-60'] +
        col('ar_61_90_days_amount') * ar_weights['61-90'] +
        col('ar_91_180_days_amount') * ar_weights['91-180'] +
        col('ar_over_180_days_amount') * ar_weights['>181']
    )
    
    # Amount score with division by zero protection
    ar_amt_score = when(col('total_ar_amount') != 0, 
                        (ar_amt_weighted / col('total_ar_amount')) * 100) \
                   .otherwise(0)
    
    # Calculate total count
    ar_total_count = (
        col('ar_0_30_days_count') + 
        col('ar_31_60_days_count') + 
        col('ar_61_90_days_count') + 
        col('ar_91_180_days_count') +
        col('ar_over_180_days_count')
    )
    
    # Calculate weighted count
    ar_cnt_weighted = (
        col('ar_0_30_days_count') * ar_weights['0-30'] +
        col('ar_31_60_days_count') * ar_weights['31-60'] +
        col('ar_61_90_days_count') * ar_weights['61-90'] +
        col('ar_91_180_days_count') * ar_weights['91-180'] +
        col('ar_over_180_days_count') * ar_weights['>181']
    )
    
    # Count score with division by zero protection
    ar_cnt_score = when(ar_total_count != 0,
                        (ar_cnt_weighted / ar_total_count) * 100) \
                   .otherwise(0)
    
    # Combined: 70% amount, 30% count
    return (ar_amt_score * 0.7 + ar_cnt_score * 0.3)

def calculate_ap_aging_score(df, ap_weights={'0-30': 0.2, '31-60': 0.4, '61-90': 0.7, '91-180': 0.9, '>181': 1.0}):
    """
    Calculate AP aging score based on aging buckets
    Note: Longer AP aging is BETTER for cashflow
    Age groups: 0-30, 31-60, 61-90, 91-180, >181
    """
    # Calculate weighted amount
    ap_amt_weighted = (
        col('ap_0_30_days_amount') * ap_weights['0-30'] +
        col('ap_31_60_days_amount') * ap_weights['31-60'] +
        col('ap_61_90_days_amount') * ap_weights['61-90'] +
        col('ap_91_180_days_amount') * ap_weights['91-180'] +
        col('ap_over_180_days_amount') * ap_weights['>181']
    )
    
    # Amount score with division by zero protection
    ap_amt_score = when(col('total_ap_amount') != 0,
                        (ap_amt_weighted / col('total_ap_amount')) * 100) \
                   .otherwise(0)
    
    # Calculate total count
    ap_total_count = (
        col('ap_0_30_days_count') + 
        col('ap_31_60_days_count') + 
        col('ap_61_90_days_count') + 
        col('ap_91_180_days_count') +
        col('ap_over_180_days_count')
    )
    
    # Calculate weighted count
    ap_cnt_weighted = (
        col('ap_0_30_days_count') * ap_weights['0-30'] +
        col('ap_31_60_days_count') * ap_weights['31-60'] +
        col('ap_61_90_days_count') * ap_weights['61-90'] +
        col('ap_91_180_days_count') * ap_weights['91-180'] +
        col('ap_over_180_days_count') * ap_weights['>181']
    )
    
    # Count score with division by zero protection
    ap_cnt_score = when(ap_total_count != 0,
                        (ap_cnt_weighted / ap_total_count) * 100) \
                   .otherwise(0)
    
    # Combined: 70% amount, 30% count
    return (ap_amt_score * 0.7 + ap_cnt_score * 0.3)

def calculate_pl_component_score(actual, target):
    """
    Calculate P&L component score
    If actual <= target: proportional score
    If actual > target: 100 + bonus (capped at +50)
    """
    return when(actual <= target, 
                when(target != 0, (actual / target) * 100).otherwise(0)) \
           .otherwise(
                when(target != 0,
                     least(100 + ((actual - target) / target * 50), 150)) \
                .otherwise(100)
           )

print("✅ Health score calculation functions loaded")
print("⚠️  Note: Using Xero data source (financial data only)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. AR/AP Aggregates <a id='ar-ap'></a>
# 
# **Data Source**: gold.fact_receivables and gold.fact_payables

# CELL ********************

# ETL 11: AGG_AR_AP_DAILY - Calculate daily AR/AP with aging from Xero data
print("Starting AR aggregation from Xero gold layer...")

# Read latest AR snapshot
ar_snapshot = spark.table(f"{GOLD_SCHEMA}.fact_receivables")
    # .where(col('snapshot_date_key') == (
    #     spark.table(f"{GOLD_SCHEMA}.fact_receivables")
    #     .agg(max('snapshot_date_key').alias('max_date'))
    #     .collect()[0]['max_date']
    # ))

# Read dim_contact for location mapping (use first contact as default location)
dim_contact = spark.table(f"{GOLD_SCHEMA}.dim_contact").where(col('is_current') == True)
default_location_key = 775265476  # Default location key to match existing schema

# Aggregate AR by date with aging buckets
ar_agg = ar_snapshot.groupBy('invoice_date_key', 'contact_key').agg(
    # AR aging amounts
    sum(when(col('days_outstanding') <= 30, col('amount_due')).otherwise(0)).alias('ar_0_30_days_amount'),
    sum(when((col('days_outstanding') > 30) & (col('days_outstanding') <= 60), col('amount_due')).otherwise(0)).alias('ar_31_60_days_amount'),
    sum(when((col('days_outstanding') > 60) & (col('days_outstanding') <= 90), col('amount_due')).otherwise(0)).alias('ar_61_90_days_amount'),
    sum(when((col('days_outstanding') > 90) & (col('days_outstanding') <= 180), col('amount_due')).otherwise(0)).alias('ar_91_180_days_amount'),
    sum(when(col('days_outstanding') > 180, col('amount_due')).otherwise(0)).alias('ar_over_180_days_amount'),
    
    # AR aging counts
    count(when(col('days_outstanding') <= 30, 1)).alias('ar_0_30_days_count'),
    count(when((col('days_outstanding') > 30) & (col('days_outstanding') <= 60), 1)).alias('ar_31_60_days_count'),
    count(when((col('days_outstanding') > 60) & (col('days_outstanding') <= 90), 1)).alias('ar_61_90_days_count'),
    count(when((col('days_outstanding') > 90) & (col('days_outstanding') <= 180), 1)).alias('ar_91_180_days_count'),
    count(when(col('days_outstanding') > 180, 1)).alias('ar_over_180_days_count'),
    
    # Totals
    sum('amount_due').alias('total_ar_amount'),
    count('*').alias('total_ar_count'),
    avg('days_outstanding').alias('avg_ar_days')
).withColumn('location_key', lit(default_location_key))

# Add required columns for schema compatibility
ar_agg_daily = ar_agg \
    .withColumn('inventory_key', lit(None).cast('bigint')) \
    .withColumn('invoice_id', lit(None).cast('string')) \
    .withColumn('corporate_id', lit(None).cast('string')) \
    .withColumn('created_at', current_timestamp()) \
    .withColumn('updated_at', current_timestamp())

# Calculate health scores
ar_agg_daily = ar_agg_daily.withColumn('ar_score', 
    (calculate_ar_aging_score(ar_agg_daily) * 0.5)
)

# Write to table
ar_agg_daily.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{GOLD_SCHEMA}.AGG_AR")
display(ar_agg_daily)
print(f"✅ AGG_AR created with {ar_agg_daily.count()} rows")
# ar_agg_daily.select('snapshot_date_key', 'total_ar_amount', 'ar_ap_score').show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ETL 11: AGG_AR_AP_DAILY - Calculate daily AR/AP with aging from Xero data
print("Starting AP aggregation from Xero gold layer...")

# Read latest AP snapshot
ap_snapshot = spark.table(f"{GOLD_SCHEMA}.fact_payables")
    # .where(col('snapshot_date_key') == (
    #     spark.table(f"{GOLD_SCHEMA}.fact_payables")
    #     .agg(max('snapshot_date_key').alias('max_date'))
    #     .collect()[0]['max_date']
    # ))
# Read dim_contact for location mapping (use first contact as default location)
dim_contact = spark.table(f"{GOLD_SCHEMA}.dim_contact").where(col('is_current') == True)
default_location_key = 775265476  # Default location key to match existing schema

# Aggregate AP by date with aging buckets
ap_agg = ap_snapshot.groupBy('invoice_date_key', 'contact_key').agg(
    # AP aging amounts
    sum(when(col('days_overdue') <= 30, col('amount_due')).otherwise(0)).alias('ap_0_30_days_amount'),
    sum(when((col('days_overdue') > 30) & (col('days_overdue') <= 60), col('amount_due')).otherwise(0)).alias('ap_31_60_days_amount'),
    sum(when((col('days_overdue') > 60) & (col('days_overdue') <= 90), col('amount_due')).otherwise(0)).alias('ap_61_90_days_amount'),
    sum(when((col('days_overdue') > 90) & (col('days_overdue') <= 180), col('amount_due')).otherwise(0)).alias('ap_91_180_days_amount'),
    sum(when(col('days_overdue') > 180, col('amount_due')).otherwise(0)).alias('ap_over_180_days_amount'),
    
    # AP aging counts
    count(when(col('days_overdue') <= 30, 1)).alias('ap_0_30_days_count'),
    count(when((col('days_overdue') > 30) & (col('days_overdue') <= 60), 1)).alias('ap_31_60_days_count'),
    count(when((col('days_overdue') > 60) & (col('days_overdue') <= 90), 1)).alias('ap_61_90_days_count'),
    count(when((col('days_overdue') > 90) & (col('days_overdue') <= 180), 1)).alias('ap_91_180_days_count'),
    count(when(col('days_overdue') > 180, 1)).alias('ap_over_180_days_count'),
    
    # Totals
    sum('amount_due').alias('total_ap_amount'),
    count('*').alias('total_ap_count'),
    avg('days_overdue').alias('avg_ap_days')
).withColumn('location_key', lit(default_location_key))

# Add required columns for schema compatibility
ap_agg_daily = ap_agg \
    .withColumn('inventory_key', lit(None).cast('bigint')) \
    .withColumn('invoice_id', lit(None).cast('string')) \
    .withColumn('corporate_id', lit(None).cast('string')) \
    .withColumn('created_at', current_timestamp()) \
    .withColumn('updated_at', current_timestamp())

# Calculate health scores
ap_agg_daily = ap_agg_daily.withColumn('ap_score', 
    (calculate_ap_aging_score(ap_agg_daily) * 0.5)
)

# Write to table
ap_agg_daily.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{GOLD_SCHEMA}.AGG_AP")
display(ap_agg_daily)
print(f"✅ AGG_AR created with {ap_agg_daily.count()} rows")
# ap_agg_daily.select('snapshot_date_key', 'total_ap_amount', 'ap_score').show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. P&L Aggregates <a id='pl'></a>
# 
# **Data Source**: gold.fact_revenue and gold.fact_expenses

# CELL ********************

# ETL 12: AGG_PL_DAILY - Calculate daily P&L from Xero data
print("Starting P&L aggregation from Xero gold layer...")

# Read revenue facts
revenue_df = spark.table(f"{GOLD_SCHEMA}.fact_revenue") \
    .where(col('invoice_status').isin(['AUTHORISED', 'PAID']))

# Read expense facts
expenses_df = spark.table(f"{GOLD_SCHEMA}.fact_expenses")

# Aggregate revenue by date
revenue_agg = revenue_df.groupBy('invoice_date_key').agg(
    sum('total_amount').alias('total_revenue'),
    count('*').alias('revenue_transaction_count')
)

# Aggregate expenses by date and category
expenses_agg = expenses_df.groupBy('transaction_date_key').agg(
    sum('amount').alias('total_expenses'),
    sum(when(col('expense_category') == 'Cost of Goods Sold', col('amount')).otherwise(0)).alias('total_cogs'),
    sum(when(col('expense_category') != 'Cost of Goods Sold', col('amount')).otherwise(0)).alias('operating_expenses'),
    count('*').alias('expense_transaction_count')
)

# Join revenue and expenses
pl_daily = revenue_agg.join(
    expenses_agg,
    revenue_agg.invoice_date_key == expenses_agg.transaction_date_key,
    'full_outer'
).select(
    coalesce(revenue_agg.invoice_date_key, expenses_agg.transaction_date_key).alias('date_key'),
    coalesce(col('total_revenue'), lit(0)).alias('total_revenue'),
    coalesce(col('total_cogs'), lit(0)).alias('total_cogs'),
    coalesce(col('operating_expenses'), lit(0)).alias('operating_expenses'),
    coalesce(col('total_expenses'), lit(0)).alias('total_expenses'),
    coalesce(col('revenue_transaction_count'), lit(0)).alias('revenue_transaction_count'),
    coalesce(col('expense_transaction_count'), lit(0)).alias('expense_transaction_count')
)

# Calculate P&L metrics
pl_daily = pl_daily \
    .withColumn('gross_profit', col('total_revenue') - col('total_cogs')) \
    .withColumn('operating_profit', col('total_revenue') - col('operating_expenses')) \
    .withColumn('net_profit', col('total_revenue') - col('total_expenses')) \
    .withColumn('gross_margin_pct', 
        when(col('total_revenue') != 0, (col('gross_profit') / col('total_revenue')) * 100).otherwise(0)) \
    .withColumn('operating_margin_pct',
        when(col('total_revenue') != 0, (col('operating_profit') / col('total_revenue')) * 100).otherwise(0)) \
    .withColumn('net_margin_pct',
        when(col('total_revenue') != 0, (col('net_profit') / col('total_revenue')) * 100).otherwise(0))

# Add required columns for schema compatibility
pl_daily = pl_daily \
    .withColumn('location_key', lit(default_location_key)) \
    .withColumn('inventory_key', lit(None).cast('bigint')) \
    .withColumn('target_revenue', lit(0).cast('decimal(18,2)')) \
    .withColumn('target_cogs', lit(0).cast('decimal(18,2)')) \
    .withColumn('target_operating_expenses', lit(0).cast('decimal(18,2)')) \
    .withColumn('created_at', current_timestamp()) \
    .withColumn('updated_at', current_timestamp())

# Calculate P&L score (simplified - based on profitability)
pl_daily = pl_daily.withColumn('pl_score',
    when(col('total_revenue') > 0,
        least((col('net_margin_pct') + 50), lit(100))  # Convert margin to 0-100 score
    ).otherwise(0)
)
display(pl_daily)
# Write to table
# pl_daily.write \
#     .format('delta') \
#     .mode('overwrite') \
#     .option('overwriteSchema', 'true') \
#     .saveAsTable(f"{GOLD_SCHEMA}.AGG_PL_DAILY")

print(f"✅ AGG_PL_DAILY created with {pl_daily.count()} rows")
# pl_daily.select('date_key', 'total_revenue', 'total_expenses', 'net_profit', 'pl_score').show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col,
    lit,
    sum as _sum,
    when,
    coalesce,
    current_timestamp,
    least
)
ZERO = lit(0)
ONE_HUNDRED = lit(100)
ONE_FIFTY = lit(150)
FIFTY = lit(50)

TARGET_REVENUE = lit(1000)
TARGET_COGS = lit(500)
TARGET_OPEX = lit(300)
TARGET_OPERATING_PROFIT = lit(200)

def calculate_pl_component_score(actual_col, target_col):
    """
    Spark-safe P&L scoring logic.
    ALL inputs and outputs are Columns.
    """

    return (
        when(
            actual_col <= target_col,
            when(
                target_col != ZERO,
                (actual_col / target_col) * ONE_HUNDRED
            ).otherwise(ZERO)
        )
        .otherwise(
            when(
                target_col != ZERO,
                least(
                    ONE_HUNDRED + ((actual_col - target_col) / target_col * FIFTY),
                    ONE_FIFTY
                )
            ).otherwise(ONE_HUNDRED)
        )
        .cast("decimal(5,2)")
    )
print("Starting P&L aggregation from Xero gold layer...")

revenue_df = (
    spark.table(f"{GOLD_SCHEMA}.fact_revenue")
    .where(col("invoice_status").isin(["AUTHORISED", "PAID"]))
    .select(
        col("invoice_id"),
        col("invoice_date_key"),
        col("line_item_id").alias("inventory_key"),
        col("line_amount").alias("line_revenue")
    )
)

expenses_df = spark.table(f"{GOLD_SCHEMA}.fact_expenses")

cogs_daily = (
    expenses_df
    .where(col("expense_category") == lit("Cost of Goods Sold"))
    .groupBy("transaction_date_key")
    .agg(_sum(col("amount")).alias("daily_cogs"))
)

opex_daily = (
    expenses_df
    .where(col("expense_category") != lit("Cost of Goods Sold"))
    .groupBy("transaction_date_key")
    .agg(_sum(col("amount")).alias("daily_opex"))
)

revenue_daily = (
    revenue_df
    .groupBy("invoice_date_key")
    .agg(_sum(col("line_revenue")).alias("daily_revenue"))
)

pl_joined = (
    revenue_df
    .join(revenue_daily, "invoice_date_key", "left")
    .join(
        cogs_daily,
        revenue_df.invoice_date_key == cogs_daily.transaction_date_key,
        "left"
    )
    .join(
        opex_daily,
        revenue_df.invoice_date_key == opex_daily.transaction_date_key,
        "left"
    )
)

pl_allocated = (
    pl_joined
    .select(
        col("invoice_id"),
        col("invoice_date_key"),
        col("inventory_key"),
        col("line_revenue"),
        coalesce(col("daily_revenue"), ZERO).alias("daily_revenue"),
        coalesce(col("daily_cogs"), ZERO).alias("daily_cogs"),
        coalesce(col("daily_opex"), ZERO).alias("daily_opex"),
        when(col("daily_revenue") != ZERO,
             col("line_revenue") / col("daily_revenue"))
        .otherwise(ZERO)
        .alias("allocation_ratio")
    )
    .select(
        col("invoice_id"),
        col("invoice_date_key"),
        col("inventory_key"),
        col("line_revenue"),
        (col("daily_cogs") * col("allocation_ratio")).alias("allocated_cogs"),
        (col("daily_opex") * col("allocation_ratio")).alias("allocated_opex")
    )
)

agg_pl = (
    pl_allocated
    .select(
        lit(default_location_key).alias("location_key"),
        col("invoice_id"),
        col("invoice_date_key"),
        col("inventory_key"),

        # Revenue
        col("line_revenue").alias("total_revenue"),
        lit(None).cast("decimal(18,2)").alias("target_revenue"),
        col("line_revenue").alias("revenue_variance"),
        ZERO.cast("decimal(5,2)").alias("revenue_variance_pct"),

        # Cost of Sales
        col("allocated_cogs").alias("cost_of_sales"),
        lit(None).cast("decimal(18,2)").alias("target_cogs"),
        col("allocated_cogs").alias("cogs_variance"),

        # Gross Profit
        (col("line_revenue") - col("allocated_cogs")).alias("gross_profit"),
        when(col("line_revenue") != ZERO,
             (col("line_revenue") - col("allocated_cogs")) /
             col("line_revenue") * ONE_HUNDRED)
        .otherwise(ZERO)
        .cast("decimal(5,2)").alias("gross_profit_margin"),

        # OPEX
        col("allocated_opex").alias("operating_expenses"),
        lit(None).cast("decimal(18,2)").alias("target_opex"),
        col("allocated_opex").alias("opex_variance"),

        # Operating Profit
        (col("line_revenue") - col("allocated_cogs") - col("allocated_opex"))
        .alias("operating_profit"),
        when(col("line_revenue") != ZERO,
             (col("line_revenue") - col("allocated_cogs") - col("allocated_opex")) /
             col("line_revenue") * ONE_HUNDRED)
        .otherwise(ZERO)
        .cast("decimal(5,2)").alias("operating_profit_margin")
    )
)

agg_pl_scored = (
    agg_pl
    .withColumn(
        "revenue_score",
        calculate_pl_component_score(col("total_revenue"), TARGET_REVENUE)
    )
    .withColumn(
        "cogs_score",
        calculate_pl_component_score(TARGET_COGS - col("cost_of_sales"), TARGET_COGS)
    )
    .withColumn(
        "gross_profit_score",
        calculate_pl_component_score(col("gross_profit"), TARGET_COGS)
    )
    .withColumn(
        "opex_score",
        calculate_pl_component_score(TARGET_OPEX - col("operating_expenses"), TARGET_OPEX)
    )
    .withColumn(
        "operating_profit_score",
        calculate_pl_component_score(col("operating_profit"), TARGET_OPERATING_PROFIT)
    )
    .withColumn(
        "pl_health_score",
        (
            col("revenue_score") * lit(0.5) +
            col("cogs_score") * lit(0.1) +
            col("gross_profit_score") * lit(0.1) +
            col("opex_score") * lit(0.2) +
            col("operating_profit_score") * lit(0.1)
        ).cast("decimal(5,2)")
    )
)

agg_pl_final = (
    agg_pl_scored
    .withColumn("dw_created_at", current_timestamp())
    .withColumn("dw_updated_at", current_timestamp())
)

display(agg_pl_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Cashflow Aggregates <a id='cashflow'></a>
# 
# **Data Source**: Calculated from revenue and expenses

# CELL ********************

df = spark.sql("SELECT * FROM lh_bnj_gold.gold.agg_pl LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ETL 13: AGG_CASHFLOW_DAILY - Calculate daily cashflow
print("Starting Cashflow aggregation...")

# Use revenue (cash in) and expenses (cash out) as proxies
cashflow_daily = pl_daily.select(
    col('date_key'),
    col('location_key'),
    col('total_revenue').alias('cash_in'),
    col('total_expenses').alias('cash_out'),
    (col('total_revenue') - col('total_expenses')).alias('net_cashflow'),
    col('revenue_transaction_count').alias('cash_in_count'),
    col('expense_transaction_count').alias('cash_out_count')
).withColumn('inventory_key', lit(None).cast('bigint')) \
 .withColumn('created_at', current_timestamp()) \
 .withColumn('updated_at', current_timestamp())

# Calculate cashflow score (positive cashflow = good)
cashflow_daily = cashflow_daily.withColumn('cashflow_score',
    when(col('cash_in') > 0,
        least(((col('net_cashflow') / col('cash_in')) * 100) + 50, lit(100))
    ).otherwise(0)
)

# Write to table
cashflow_daily.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{GOLD_SCHEMA}.AGG_CASHFLOW_DAILY")

print(f"✅ AGG_CASHFLOW_DAILY created with {cashflow_daily.count()} rows")
cashflow_daily.select('date_key', 'cash_in', 'cash_out', 'net_cashflow', 'cashflow_score').show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Inventory Aggregates <a id='inventory'></a>
# 
# **⚠️ Note**: Xero does not track inventory. Creating stub table with NULL values for schema compatibility.

# CELL ********************

# ETL 14: AGG_INVENTORY_DAILY - Create stub with NULL values
print("Creating stub inventory table (Xero has no inventory data)...")

# Create empty stub table with schema
inventory_schema = StructType([
    StructField('date_key', IntegerType(), False),
    StructField('location_key', LongType(), False),
    StructField('inventory_key', LongType(), True),
    StructField('total_stock_value', DecimalType(18,2), True),
    StructField('items_in_stock', IntegerType(), True),
    StructField('items_low_stock', IntegerType(), True),
    StructField('items_out_of_stock', IntegerType(), True),
    StructField('inventory_turnover', DecimalType(10,2), True),
    StructField('inventory_score', DecimalType(5,2), True),
    StructField('created_at', TimestampType(), False),
    StructField('updated_at', TimestampType(), False)
])

# Create single row with current date and NULL values
current_date_key = int(datetime.now().strftime('%Y%m%d'))
inventory_stub = spark.createDataFrame([
    (current_date_key, default_location_key, None, None, None, None, None, None, None, datetime.now(), datetime.now())
], inventory_schema)

# Write to table
inventory_stub.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{GOLD_SCHEMA}.AGG_INVENTORY_DAILY")

print(f"✅ AGG_INVENTORY_DAILY created (stub with NULL values)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Claims Aggregates <a id='claims'></a>
# 
# **⚠️ Note**: Xero does not track medical claims. Creating stub table with NULL values for schema compatibility.

# CELL ********************

# ETL 15: AGG_CLAIMS_DAILY - Create stub with NULL values
print("Creating stub claims table (Xero has no claims data)...")

# Create empty stub table with schema
claims_schema = StructType([
    StructField('date_key', IntegerType(), False),
    StructField('location_key', LongType(), False),
    StructField('total_claims', IntegerType(), True),
    StructField('approved_claims', IntegerType(), True),
    StructField('rejected_claims', IntegerType(), True),
    StructField('pending_claims', IntegerType(), True),
    StructField('total_claim_amount', DecimalType(18,2), True),
    StructField('approved_claim_amount', DecimalType(18,2), True),
    StructField('approval_rate', DecimalType(5,2), True),
    StructField('avg_processing_days', DecimalType(10,2), True),
    StructField('claims_score', DecimalType(5,2), True),
    StructField('created_at', TimestampType(), False),
    StructField('updated_at', TimestampType(), False)
])

claims_stub = spark.createDataFrame([
    (current_date_key, default_location_key, None, None, None, None, None, None, None, None, None, datetime.now(), datetime.now())
], claims_schema)

# Write to table
claims_stub.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{GOLD_SCHEMA}.AGG_CLAIMS_DAILY")

print(f"✅ AGG_CLAIMS_DAILY created (stub with NULL values)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Patient Aggregates <a id='patient'></a>
# 
# **⚠️ Note**: Xero does not track patient visits. Creating stub table with NULL values for schema compatibility.

# CELL ********************

# ETL 16: AGG_PATIENT_DAILY - Create stub with NULL values
print("Creating stub patient table (Xero has no patient visit data)...")

# Create empty stub table with schema
patient_schema = StructType([
    StructField('date_key', IntegerType(), False),
    StructField('location_key', LongType(), False),
    StructField('total_patients', IntegerType(), True),
    StructField('new_patients', IntegerType(), True),
    StructField('returning_patients', IntegerType(), True),
    StructField('total_visits', IntegerType(), True),
    StructField('avg_revenue_per_patient', DecimalType(18,2), True),
    StructField('patient_satisfaction', DecimalType(5,2), True),
    StructField('patient_score', DecimalType(5,2), True),
    StructField('created_at', TimestampType(), False),
    StructField('updated_at', TimestampType(), False)
])

patient_stub = spark.createDataFrame([
    (current_date_key, default_location_key, None, None, None, None, None, None, None, datetime.now(), datetime.now())
], patient_schema)

# Write to table
patient_stub.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{GOLD_SCHEMA}.AGG_PATIENT_DAILY")

print(f"✅ AGG_PATIENT_DAILY created (stub with NULL values)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Health Score Composites <a id='health-scores'></a>
# 
# **Note**: Only Financial Health Score is calculated (Operational Health is NULL due to no inventory/claims/patient data)

# CELL ********************

# ETL 17: AGG_HEALTH_SCORES_DAILY - Combine all health scores
print("Calculating composite health scores...")

# Read all aggregate tables
ar_ap = spark.table(f"{GOLD_SCHEMA}.AGG_AR_AP_DAILY")
pl = spark.table(f"{GOLD_SCHEMA}.AGG_PL_DAILY")
cashflow = spark.table(f"{GOLD_SCHEMA}.AGG_CASHFLOW_DAILY")

# Join all on date_key
health_scores = ar_ap.alias('ar') \
    .join(pl.alias('pl'), 
          (col('ar.snapshot_date_key') == col('pl.date_key')) & 
          (col('ar.location_key') == col('pl.location_key')), 
          'full_outer') \
    .join(cashflow.alias('cf'),
          (coalesce(col('ar.snapshot_date_key'), col('pl.date_key')) == col('cf.date_key')) &
          (coalesce(col('ar.location_key'), col('pl.location_key')) == col('cf.location_key')),
          'full_outer')

# Select and calculate composite scores
health_scores = health_scores.select(
    coalesce(col('ar.snapshot_date_key'), col('pl.date_key'), col('cf.date_key')).alias('date_key'),
    coalesce(col('ar.location_key'), col('pl.location_key'), col('cf.location_key')).alias('location_key'),
    
    # Individual component scores
    coalesce(col('ar.ar_ap_score'), lit(0)).alias('ar_ap_score'),
    coalesce(col('pl.pl_score'), lit(0)).alias('pl_score'),
    coalesce(col('cf.cashflow_score'), lit(0)).alias('cashflow_score'),
    lit(None).cast('decimal(5,2)').alias('inventory_score'),  # NULL - no data
    lit(None).cast('decimal(5,2)').alias('claims_score'),     # NULL - no data
    lit(None).cast('decimal(5,2)').alias('patient_score')     # NULL - no data
)

# Calculate Financial Health Score
# Formula: 50% P&L + 25% AR/AP + 25% Cashflow
health_scores = health_scores.withColumn('financial_health_score',
    (col('pl_score') * 0.50) + 
    (col('ar_ap_score') * 0.25) + 
    (col('cashflow_score') * 0.25)
)

# Operational Health Score = NULL (no inventory, claims, or patient data)
health_scores = health_scores.withColumn('operational_health_score', lit(None).cast('decimal(5,2)'))

# Overall Health Score = Financial Health only (since Operational is NULL)
health_scores = health_scores.withColumn('overall_health_score', col('financial_health_score'))

# Add metadata
health_scores = health_scores \
    .withColumn('inventory_key', lit(None).cast('bigint')) \
    .withColumn('created_at', current_timestamp()) \
    .withColumn('updated_at', current_timestamp())

# Write to table
health_scores.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{GOLD_SCHEMA}.AGG_HEALTH_SCORES_DAILY")

print(f"✅ AGG_HEALTH_SCORES_DAILY created with {health_scores.count()} rows")
print("\n📊 Health Scores Summary:")
health_scores.select(
    'date_key',
    'ar_ap_score',
    'pl_score',
    'cashflow_score',
    'financial_health_score',
    'operational_health_score',
    'overall_health_score'
).show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary & Verification

# CELL ********************

# Verification summary
print("="*80)
print("ETL PART 2 - AGGREGATES & HEALTH SCORES COMPLETE")
print("="*80)

tables = [
    'AGG_AR_AP_DAILY',
    'AGG_PL_DAILY',
    'AGG_CASHFLOW_DAILY',
    'AGG_INVENTORY_DAILY',
    'AGG_CLAIMS_DAILY',
    'AGG_PATIENT_DAILY',
    'AGG_HEALTH_SCORES_DAILY'
]

for table in tables:
    count = spark.table(f"{GOLD_SCHEMA}.{table}").count()
    status = "✅" if count > 0 else "⚠️"
    print(f"{status} {table}: {count} rows")

print("\n" + "="*80)
print("📝 NOTES:")
print("  • Financial health scores calculated from Xero data")
print("  • Operational health scores = NULL (no inventory/claims/patient data in Xero)")
print("  • All column names match original schema for application compatibility")
print("  • Stub tables created for inventory, claims, and patient to maintain schema")
print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
