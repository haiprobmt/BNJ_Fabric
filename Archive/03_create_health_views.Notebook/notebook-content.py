# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # BNJ Medical CFO Dashboard - Health Score Views
# 
# This notebook creates views for the 10 Health Metrics and aggregated health scores.
# 
# **Health Metrics (10% each):**
# 1. YoY Health
# 2. Financial Health
# 3. Operational Health
# 4. Performance to Target
# 5. P&L Health
# 6. AR/AP Health
# 7. Cashflow Health
# 8. Inventory Score
# 9. Claims Score
# 10. Patient Score

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, date

spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration - Targets and Weights

# CELL ********************

# Performance Targets (can be loaded from config table)
TARGETS = {
    "revenue_target_monthly": 200000,          # $200K monthly revenue target
    "collection_target_pct": 95.0,             # 95% collection rate target
    "claim_approval_target_pct": 90.0,         # 90% claim approval rate
    "ar_days_target": 30,                      # AR days < 30
    "ap_days_target": 45,                      # AP days < 45
    "inventory_turnover_target": 6.0,          # 6 turns per year
    "patient_retention_target_pct": 85.0,      # 85% retention
    "no_show_target_pct": 5.0,                 # < 5% no-shows
    "gross_margin_target_pct": 60.0,           # 60% gross margin
    "operating_margin_target_pct": 20.0,       # 20% operating margin
    "current_ratio_target": 1.5,               # Current ratio > 1.5
    "cash_runway_months_target": 3             # 3 months cash runway
}

# Health score weights (equal weighting = 10% each)
WEIGHTS = {
    "yoy_health": 0.10,
    "financial_health": 0.10,
    "operational_health": 0.10,
    "performance_target": 0.10,
    "pnl_health": 0.10,
    "arap_health": 0.10,
    "cashflow_health": 0.10,
    "inventory_score": 0.10,
    "claims_score": 0.10,
    "patient_score": 0.10
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Functions

# CELL ********************

def score_to_100(value, target, higher_is_better=True):
    """
    Convert a metric to a 0-100 score based on target.
    Score = 100 when value meets/exceeds target.
    """
    if higher_is_better:
        return least(lit(100), (col(value) / lit(target)) * 100)
    else:
        return least(lit(100), (lit(target) / col(value)) * 100)

def safe_divide(numerator, denominator, default=0):
    """Safe division handling null/zero"""
    return when(col(denominator) != 0, 
                col(numerator) / col(denominator)).otherwise(lit(default))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. YoY Health Score
# 
# Measures year-over-year growth trends.

# CELL ********************

def create_vw_yoy_health():
    """
    YoY Health: Compares current period to same period last year.
    Components:
    - Revenue growth
    - Profit growth
    - Patient volume growth
    """
    
    # Get current and previous year metrics
    fact_invoice = spark.table("fact_invoice")
    dim_date = spark.table("dim_date")
    
    # Join with date dimension
    invoice_dated = fact_invoice.join(
        dim_date.select("date_key", "year", "month", "fiscal_year", "fiscal_quarter"),
        "date_key"
    )
    
    # Monthly revenue by year
    monthly_metrics = invoice_dated.groupBy("year", "month").agg(
        sum("net_amount").alias("revenue"),
        sum("profit_amount").alias("profit"),
        countDistinct("patient_key").alias("patient_count")
    )
    
    # Create YoY comparison
    current_year = monthly_metrics.alias("current")
    prev_year = monthly_metrics.alias("prev")
    
    yoy_comparison = current_year.join(
        prev_year,
        (col("current.year") == col("prev.year") + 1) & 
        (col("current.month") == col("prev.month")),
        "left"
    ).select(
        col("current.year").alias("year"),
        col("current.month").alias("month"),
        col("current.revenue").alias("current_revenue"),
        col("prev.revenue").alias("prior_revenue"),
        col("current.profit").alias("current_profit"),
        col("prev.profit").alias("prior_profit"),
        col("current.patient_count").alias("current_patients"),
        col("prev.patient_count").alias("prior_patients")
    )
    
    # Calculate growth rates and scores
    yoy_health = yoy_comparison.select(
        col("year"),
        col("month"),
        col("current_revenue"),
        col("prior_revenue"),
        when(col("prior_revenue") > 0,
             ((col("current_revenue") - col("prior_revenue")) / col("prior_revenue")) * 100
        ).otherwise(lit(0)).alias("revenue_growth_pct"),
        when(col("prior_profit") > 0,
             ((col("current_profit") - col("prior_profit")) / col("prior_profit")) * 100
        ).otherwise(lit(0)).alias("profit_growth_pct"),
        when(col("prior_patients") > 0,
             ((col("current_patients") - col("prior_patients")) / col("prior_patients")) * 100
        ).otherwise(lit(0)).alias("patient_growth_pct")
    )
    
    # Score: Target is 10% growth, max score at 20%+ growth
    yoy_health = yoy_health.withColumn(
        "revenue_growth_score",
        least(lit(100), greatest(lit(0), (col("revenue_growth_pct") + 10) * 5))
    ).withColumn(
        "profit_growth_score",
        least(lit(100), greatest(lit(0), (col("profit_growth_pct") + 10) * 5))
    ).withColumn(
        "patient_growth_score",
        least(lit(100), greatest(lit(0), (col("patient_growth_pct") + 10) * 5))
    ).withColumn(
        "yoy_health_score",
        (col("revenue_growth_score") * 0.4 +
         col("profit_growth_score") * 0.4 +
         col("patient_growth_score") * 0.2)
    )
    
    return yoy_health

# Create view
try:
    yoy_health_df = create_vw_yoy_health()
    yoy_health_df.createOrReplaceTempView("vw_yoy_health")
    yoy_health_df.write.format("delta").mode("overwrite").saveAsTable("vw_yoy_health")
    print("✅ Created vw_yoy_health")
except Exception as e:
    print(f"⚠️ Could not create vw_yoy_health: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Financial Health Score
# 
# Overall financial health combining P&L, AR/AP, and Cashflow.

# CELL ********************

def create_vw_financial_health():
    """
    Financial Health: Composite of P&L, AR/AP, and Cashflow health.
    This is a rollup view that combines the three finance dashboard metrics.
    """
    
    # Get component health scores (will be created below)
    # For now, calculate directly
    
    # P&L Health from invoices
    fact_invoice = spark.table("fact_invoice")
    dim_date = spark.table("dim_date")
    
    invoice_dated = fact_invoice.join(
        dim_date.select("date_key", "year", "month"),
        "date_key"
    )
    
    pnl_metrics = invoice_dated.groupBy("year", "month").agg(
        sum("net_amount").alias("revenue"),
        sum("cost_amount").alias("cogs"),
        sum("profit_amount").alias("gross_profit")
    ).withColumn(
        "gross_margin_pct",
        when(col("revenue") > 0, (col("gross_profit") / col("revenue")) * 100).otherwise(lit(0))
    )
    
    # AR Health from AR aging
    fact_ar = spark.table("fact_ar_aging")
    ar_metrics = fact_ar.groupBy().agg(
        sum("outstanding_amount").alias("total_ar"),
        sum(when(col("is_overdue"), col("outstanding_amount"))).alias("overdue_ar")
    ).withColumn(
        "ar_current_pct",
        when(col("total_ar") > 0, 
             ((col("total_ar") - col("overdue_ar")) / col("total_ar")) * 100
        ).otherwise(lit(100))
    )
    
    # Cashflow Health
    fact_cashflow = spark.table("fact_cashflow")
    cashflow_metrics = fact_cashflow.groupBy().agg(
        sum("cash_in").alias("total_cash_in"),
        sum("cash_out").alias("total_cash_out"),
        max("running_balance").alias("cash_balance")
    ).withColumn(
        "cashflow_ratio",
        when(col("total_cash_out") > 0,
             col("total_cash_in") / col("total_cash_out")
        ).otherwise(lit(1))
    )
    
    # Combine metrics
    financial_health = pnl_metrics.crossJoin(ar_metrics).crossJoin(cashflow_metrics)
    
    # Calculate component scores
    financial_health = financial_health.withColumn(
        "pnl_score",
        least(lit(100), (col("gross_margin_pct") / TARGETS["gross_margin_target_pct"]) * 100)
    ).withColumn(
        "ar_score",
        col("ar_current_pct")  # Already 0-100
    ).withColumn(
        "cashflow_score",
        least(lit(100), col("cashflow_ratio") * 100)
    ).withColumn(
        "financial_health_score",
        (col("pnl_score") * 0.4 + col("ar_score") * 0.3 + col("cashflow_score") * 0.3)
    )
    
    return financial_health

# Create view
try:
    financial_health_df = create_vw_financial_health()
    financial_health_df.createOrReplaceTempView("vw_financial_health")
    financial_health_df.write.format("delta").mode("overwrite").saveAsTable("vw_financial_health")
    print("✅ Created vw_financial_health")
except Exception as e:
    print(f"⚠️ Could not create vw_financial_health: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Operational Health Score
# 
# Combines Inventory, Claims, and Patient health.

# CELL ********************

def create_vw_operational_health():
    """
    Operational Health: Composite of Inventory, Claims, and Patient scores.
    """
    
    # Inventory Health
    fact_inventory = spark.table("fact_inventory")
    inventory_metrics = fact_inventory.groupBy().agg(
        sum("quantity_in").alias("total_in"),
        sum("quantity_out").alias("total_out"),
        sum(when(col("is_expired"), col("total_value"))).alias("expired_value"),
        sum("total_value").alias("total_inventory_value")
    ).withColumn(
        "turnover_ratio",
        when(col("total_inventory_value") > 0,
             col("total_out") / (col("total_inventory_value") / 12)  # Monthly avg
        ).otherwise(lit(0))
    ).withColumn(
        "expiry_loss_pct",
        when(col("total_inventory_value") > 0,
             (col("expired_value") / col("total_inventory_value")) * 100
        ).otherwise(lit(0))
    )
    
    # Claims Health
    fact_claims = spark.table("fact_claims")
    claims_metrics = fact_claims.groupBy().agg(
        count("*").alias("total_claims"),
        sum(when(col("claim_status") == "Approved", lit(1))).alias("approved_claims"),
        avg("days_to_process").alias("avg_processing_days"),
        sum("claim_amount").alias("total_claimed"),
        sum("approved_amount").alias("total_approved")
    ).withColumn(
        "approval_rate",
        when(col("total_claims") > 0,
             (col("approved_claims") / col("total_claims")) * 100
        ).otherwise(lit(0))
    )
    
    # Patient Health
    fact_appointment = spark.table("fact_appointment")
    patient_metrics = fact_appointment.groupBy().agg(
        countDistinct("patient_key").alias("unique_patients"),
        sum(when(col("is_new_patient"), lit(1))).alias("new_patients"),
        sum(when(col("is_no_show"), lit(1))).alias("no_shows"),
        count("*").alias("total_appointments")
    ).withColumn(
        "retention_rate",
        when(col("unique_patients") > 0,
             ((col("unique_patients") - col("new_patients")) / col("unique_patients")) * 100
        ).otherwise(lit(0))
    ).withColumn(
        "no_show_rate",
        when(col("total_appointments") > 0,
             (col("no_shows") / col("total_appointments")) * 100
        ).otherwise(lit(0))
    )
    
    # Combine and calculate scores
    operational_health = inventory_metrics.crossJoin(claims_metrics).crossJoin(patient_metrics)
    
    operational_health = operational_health.withColumn(
        "inventory_score",
        least(lit(100), (col("turnover_ratio") / TARGETS["inventory_turnover_target"]) * 100) *
        (1 - (col("expiry_loss_pct") / 100))  # Penalize expired inventory
    ).withColumn(
        "claims_score",
        (col("approval_rate") / TARGETS["claim_approval_target_pct"]) * 100
    ).withColumn(
        "patient_score",
        (col("retention_rate") / TARGETS["patient_retention_target_pct"]) * 50 +
        ((TARGETS["no_show_target_pct"] - col("no_show_rate")) / TARGETS["no_show_target_pct"]) * 50
    ).withColumn(
        "operational_health_score",
        (col("inventory_score") * 0.33 + col("claims_score") * 0.33 + col("patient_score") * 0.34)
    )
    
    return operational_health

# Create view
try:
    operational_health_df = create_vw_operational_health()
    operational_health_df.createOrReplaceTempView("vw_operational_health")
    operational_health_df.write.format("delta").mode("overwrite").saveAsTable("vw_operational_health")
    print("✅ Created vw_operational_health")
except Exception as e:
    print(f"⚠️ Could not create vw_operational_health: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Performance to Target Score

# CELL ********************

def create_vw_performance_target():
    """
    Performance to Target: How actual metrics compare to predefined targets.
    """
    
    fact_invoice = spark.table("fact_invoice")
    fact_payment = spark.table("fact_payment")
    dim_date = spark.table("dim_date")
    
    # Get current month/year
    current_date_info = dim_date.filter(col("full_date") == current_date()) \
        .select("year", "month").first()
    current_year = current_date_info["year"] if current_date_info else date.today().year
    current_month = current_date_info["month"] if current_date_info else date.today().month
    
    # Monthly revenue
    invoice_dated = fact_invoice.join(
        dim_date.select("date_key", "year", "month"),
        "date_key"
    )
    
    monthly_revenue = invoice_dated.groupBy("year", "month").agg(
        sum("net_amount").alias("actual_revenue")
    ).withColumn(
        "target_revenue",
        lit(TARGETS["revenue_target_monthly"])
    ).withColumn(
        "revenue_achievement_pct",
        (col("actual_revenue") / col("target_revenue")) * 100
    )
    
    # Collection rate
    payment_dated = fact_payment.join(
        dim_date.select("date_key", "year", "month"),
        "date_key"
    )
    
    collections = payment_dated.groupBy("year", "month").agg(
        sum("payment_amount").alias("collected")
    )
    
    performance = monthly_revenue.join(collections, ["year", "month"], "left")
    
    performance = performance.withColumn(
        "collection_rate",
        when(col("actual_revenue") > 0,
             (col("collected") / col("actual_revenue")) * 100
        ).otherwise(lit(0))
    ).withColumn(
        "revenue_score",
        least(lit(100), col("revenue_achievement_pct"))
    ).withColumn(
        "collection_score",
        (col("collection_rate") / TARGETS["collection_target_pct"]) * 100
    ).withColumn(
        "performance_target_score",
        (col("revenue_score") * 0.6 + col("collection_score") * 0.4)
    )
    
    return performance

# Create view
try:
    performance_target_df = create_vw_performance_target()
    performance_target_df.createOrReplaceTempView("vw_performance_target")
    performance_target_df.write.format("delta").mode("overwrite").saveAsTable("vw_performance_target")
    print("✅ Created vw_performance_target")
except Exception as e:
    print(f"⚠️ Could not create vw_performance_target: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. P&L Health Score

# CELL ********************

def create_vw_pnl_health():
    """
    P&L Health: Profitability and margin analysis.
    """
    
    fact_invoice = spark.table("fact_invoice")
    dim_date = spark.table("dim_date")
    
    invoice_dated = fact_invoice.join(
        dim_date.select("date_key", "year", "month", "quarter"),
        "date_key"
    )
    
    pnl_health = invoice_dated.groupBy("year", "month", "quarter").agg(
        sum("gross_amount").alias("gross_revenue"),
        sum("discount_amount").alias("total_discounts"),
        sum("net_amount").alias("net_revenue"),
        sum("cost_amount").alias("cogs"),
        sum("profit_amount").alias("gross_profit")
    ).withColumn(
        "gross_margin_pct",
        when(col("net_revenue") > 0,
             (col("gross_profit") / col("net_revenue")) * 100
        ).otherwise(lit(0))
    ).withColumn(
        "discount_rate",
        when(col("gross_revenue") > 0,
             (col("total_discounts") / col("gross_revenue")) * 100
        ).otherwise(lit(0))
    ).withColumn(
        "margin_score",
        (col("gross_margin_pct") / TARGETS["gross_margin_target_pct"]) * 100
    ).withColumn(
        "discount_score",
        greatest(lit(0), lit(100) - (col("discount_rate") * 10))  # Penalize high discounts
    ).withColumn(
        "pnl_health_score",
        least(lit(100), (col("margin_score") * 0.7 + col("discount_score") * 0.3))
    )
    
    return pnl_health

# Create view
try:
    pnl_health_df = create_vw_pnl_health()
    pnl_health_df.createOrReplaceTempView("vw_pnl_health")
    pnl_health_df.write.format("delta").mode("overwrite").saveAsTable("vw_pnl_health")
    print("✅ Created vw_pnl_health")
except Exception as e:
    print(f"⚠️ Could not create vw_pnl_health: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. AR/AP Health Score

# CELL ********************

def create_vw_arap_health():
    """
    AR/AP Health: Receivables and payables aging analysis.
    """
    
    # AR Analysis
    fact_ar = spark.table("fact_ar_aging")
    ar_summary = fact_ar.groupBy("aging_bucket").agg(
        sum("outstanding_amount").alias("ar_amount"),
        count("*").alias("invoice_count")
    )
    
    ar_total = fact_ar.agg(
        sum("outstanding_amount").alias("total_ar"),
        sum(when(col("aging_bucket") == "Current", col("outstanding_amount"))).alias("ar_current"),
        sum(when(col("aging_bucket") == "90+", col("outstanding_amount"))).alias("ar_90_plus"),
        avg("days_outstanding").alias("avg_ar_days")
    )
    
    # AP Analysis
    fact_ap = spark.table("fact_ap_aging")
    ap_total = fact_ap.agg(
        sum("outstanding_amount").alias("total_ap"),
        sum(when(col("aging_bucket") == "Current", col("outstanding_amount"))).alias("ap_current"),
        sum(when(col("is_overdue"), col("outstanding_amount"))).alias("ap_overdue"),
        avg("days_outstanding").alias("avg_ap_days")
    )
    
    # Combine AR and AP
    arap_health = ar_total.crossJoin(ap_total)
    
    arap_health = arap_health.withColumn(
        "ar_current_pct",
        when(col("total_ar") > 0,
             (col("ar_current") / col("total_ar")) * 100
        ).otherwise(lit(100))
    ).withColumn(
        "ar_score",
        # Score based on % current and DSO vs target
        (col("ar_current_pct") * 0.5) +
        (least(lit(50), (TARGETS["ar_days_target"] / greatest(col("avg_ar_days"), lit(1))) * 50))
    ).withColumn(
        "ap_on_time_pct",
        when(col("total_ap") > 0,
             ((col("total_ap") - col("ap_overdue")) / col("total_ap")) * 100
        ).otherwise(lit(100))
    ).withColumn(
        "ap_score",
        col("ap_on_time_pct")
    ).withColumn(
        "arap_health_score",
        (col("ar_score") * 0.6 + col("ap_score") * 0.4)  # AR weighted higher for clinic
    )
    
    return arap_health

# Create view
try:
    arap_health_df = create_vw_arap_health()
    arap_health_df.createOrReplaceTempView("vw_arap_health")
    arap_health_df.write.format("delta").mode("overwrite").saveAsTable("vw_arap_health")
    print("✅ Created vw_arap_health")
except Exception as e:
    print(f"⚠️ Could not create vw_arap_health: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Cashflow Health Score

# CELL ********************

def create_vw_cashflow_health():
    """
    Cashflow Health: Cash position and flow analysis.
    """
    
    fact_cashflow = spark.table("fact_cashflow")
    dim_date = spark.table("dim_date")
    
    # Monthly cashflow
    cashflow_dated = fact_cashflow.join(
        dim_date.select("date_key", "year", "month"),
        "date_key"
    )
    
    monthly_cashflow = cashflow_dated.groupBy("year", "month").agg(
        sum("cash_in").alias("cash_inflow"),
        sum("cash_out").alias("cash_outflow"),
        sum("net_cashflow").alias("net_cashflow"),
        max("running_balance").alias("ending_balance")
    )
    
    # Calculate avg monthly outflow for runway
    avg_monthly_outflow = monthly_cashflow.agg(
        avg("cash_outflow").alias("avg_monthly_burn")
    )
    
    # Add runway calculation
    cashflow_health = monthly_cashflow.crossJoin(avg_monthly_outflow)
    
    cashflow_health = cashflow_health.withColumn(
        "cashflow_ratio",
        when(col("cash_outflow") > 0,
             col("cash_inflow") / col("cash_outflow")
        ).otherwise(lit(1))
    ).withColumn(
        "cash_runway_months",
        when(col("avg_monthly_burn") > 0,
             col("ending_balance") / col("avg_monthly_burn")
        ).otherwise(lit(12))  # Default 12 months if no burn
    ).withColumn(
        "inflow_outflow_score",
        least(lit(100), col("cashflow_ratio") * 100)
    ).withColumn(
        "runway_score",
        least(lit(100), 
              (col("cash_runway_months") / TARGETS["cash_runway_months_target"]) * 100)
    ).withColumn(
        "cashflow_health_score",
        (col("inflow_outflow_score") * 0.6 + col("runway_score") * 0.4)
    )
    
    return cashflow_health

# Create view
try:
    cashflow_health_df = create_vw_cashflow_health()
    cashflow_health_df.createOrReplaceTempView("vw_cashflow_health")
    cashflow_health_df.write.format("delta").mode("overwrite").saveAsTable("vw_cashflow_health")
    print("✅ Created vw_cashflow_health")
except Exception as e:
    print(f"⚠️ Could not create vw_cashflow_health: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Inventory Score

# CELL ********************

def create_vw_inventory_score():
    """
    Inventory Score: Stock management effectiveness.
    """
    
    fact_inventory = spark.table("fact_inventory")
    dim_date = spark.table("dim_date")
    dim_product = spark.table("dim_product")
    
    # Current inventory status
    inventory_current = fact_inventory.groupBy("product_key").agg(
        sum("quantity_in").alias("total_received"),
        sum("quantity_out").alias("total_issued"),
        sum("total_value").alias("inventory_value"),
        max("expiry_date").alias("latest_expiry"),
        sum(when(col("is_expired"), col("total_value"))).alias("expired_value")
    ).withColumn(
        "current_stock",
        col("total_received") - col("total_issued")
    )
    
    # Join with product info
    inventory_status = inventory_current.join(dim_product, "product_key", "left")
    
    inventory_status = inventory_status.withColumn(
        "is_low_stock",
        col("current_stock") < coalesce(col("reorder_level"), lit(10))
    ).withColumn(
        "is_overstock",
        col("current_stock") > coalesce(col("reorder_level"), lit(10)) * 3
    ).withColumn(
        "days_to_expiry",
        datediff(col("latest_expiry"), current_date())
    ).withColumn(
        "is_near_expiry",
        col("days_to_expiry") < 30
    )
    
    # Aggregate to overall score
    inventory_score = inventory_status.agg(
        count("*").alias("total_products"),
        sum(when(col("is_low_stock"), lit(1))).alias("low_stock_count"),
        sum(when(col("is_overstock"), lit(1))).alias("overstock_count"),
        sum(when(col("is_near_expiry"), lit(1))).alias("near_expiry_count"),
        sum("inventory_value").alias("total_inventory_value"),
        sum("expired_value").alias("total_expired_value")
    ).withColumn(
        "stockout_risk_pct",
        (col("low_stock_count") / col("total_products")) * 100
    ).withColumn(
        "overstock_pct",
        (col("overstock_count") / col("total_products")) * 100
    ).withColumn(
        "expiry_risk_pct",
        (col("near_expiry_count") / col("total_products")) * 100
    ).withColumn(
        "waste_pct",
        when(col("total_inventory_value") > 0,
             (col("total_expired_value") / col("total_inventory_value")) * 100
        ).otherwise(lit(0))
    ).withColumn(
        "availability_score",
        greatest(lit(0), lit(100) - (col("stockout_risk_pct") * 5))
    ).withColumn(
        "efficiency_score",
        greatest(lit(0), lit(100) - (col("overstock_pct") * 2) - (col("waste_pct") * 5))
    ).withColumn(
        "inventory_health_score",
        (col("availability_score") * 0.5 + col("efficiency_score") * 0.5)
    )
    
    return inventory_score

# Create view
try:
    inventory_score_df = create_vw_inventory_score()
    inventory_score_df.createOrReplaceTempView("vw_inventory_score")
    inventory_score_df.write.format("delta").mode("overwrite").saveAsTable("vw_inventory_score")
    print("✅ Created vw_inventory_score")
except Exception as e:
    print(f"⚠️ Could not create vw_inventory_score: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Claims Score

# CELL ********************

def create_vw_claims_score():
    """
    Claims Score: Insurance claims efficiency.
    """
    
    fact_claims = spark.table("fact_claims")
    dim_date = spark.table("dim_date")
    dim_payer = spark.table("dim_payer")
    
    # Monthly claims summary
    claims_dated = fact_claims.join(
        dim_date.select("date_key", "year", "month"),
        "date_key"
    )
    
    claims_summary = claims_dated.groupBy("year", "month").agg(
        count("*").alias("total_claims"),
        sum(when(col("claim_status") == "Approved", lit(1))).alias("approved"),
        sum(when(col("claim_status") == "Rejected", lit(1))).alias("rejected"),
        sum(when(col("claim_status") == "Pending", lit(1))).alias("pending"),
        sum("claim_amount").alias("total_claimed"),
        sum("approved_amount").alias("total_approved"),
        avg("days_to_process").alias("avg_processing_days")
    )
    
    claims_score = claims_summary.withColumn(
        "approval_rate",
        when(col("total_claims") > 0,
             (col("approved") / col("total_claims")) * 100
        ).otherwise(lit(0))
    ).withColumn(
        "rejection_rate",
        when(col("total_claims") > 0,
             (col("rejected") / col("total_claims")) * 100
        ).otherwise(lit(0))
    ).withColumn(
        "recovery_rate",
        when(col("total_claimed") > 0,
             (col("total_approved") / col("total_claimed")) * 100
        ).otherwise(lit(0))
    ).withColumn(
        "approval_score",
        (col("approval_rate") / TARGETS["claim_approval_target_pct"]) * 100
    ).withColumn(
        "processing_score",
        # Target 14 days processing, score drops after
        greatest(lit(0), lit(100) - ((col("avg_processing_days") - 14) * 3))
    ).withColumn(
        "claims_health_score",
        least(lit(100), (col("approval_score") * 0.5 + col("recovery_rate") * 0.3 + col("processing_score") * 0.2))
    )
    
    return claims_score

# Create view
try:
    claims_score_df = create_vw_claims_score()
    claims_score_df.createOrReplaceTempView("vw_claims_score")
    claims_score_df.write.format("delta").mode("overwrite").saveAsTable("vw_claims_score")
    print("✅ Created vw_claims_score")
except Exception as e:
    print(f"⚠️ Could not create vw_claims_score: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Patient Score

# CELL ********************

def create_vw_patient_score():
    """
    Patient Score: Patient engagement and satisfaction metrics.
    """
    
    fact_appointment = spark.table("fact_appointment")
    dim_date = spark.table("dim_date")
    dim_patient = spark.table("dim_patient")
    
    # Monthly patient metrics
    appt_dated = fact_appointment.join(
        dim_date.select("date_key", "year", "month"),
        "date_key"
    )
    
    patient_metrics = appt_dated.groupBy("year", "month").agg(
        countDistinct("patient_key").alias("active_patients"),
        sum(when(col("is_new_patient"), lit(1))).alias("new_patients"),
        count("*").alias("total_appointments"),
        sum(when(col("is_no_show"), lit(1))).alias("no_shows"),
        sum(when(col("is_cancelled"), lit(1))).alias("cancellations"),
        sum("revenue_generated").alias("revenue"),
        avg("duration_minutes").alias("avg_visit_duration")
    )
    
    patient_score = patient_metrics.withColumn(
        "returning_patients",
        col("active_patients") - col("new_patients")
    ).withColumn(
        "retention_rate",
        when(col("active_patients") > 0,
             (col("returning_patients") / col("active_patients")) * 100
        ).otherwise(lit(0))
    ).withColumn(
        "no_show_rate",
        when(col("total_appointments") > 0,
             (col("no_shows") / col("total_appointments")) * 100
        ).otherwise(lit(0))
    ).withColumn(
        "cancellation_rate",
        when(col("total_appointments") > 0,
             (col("cancellations") / col("total_appointments")) * 100
        ).otherwise(lit(0))
    ).withColumn(
        "revenue_per_patient",
        when(col("active_patients") > 0,
             col("revenue") / col("active_patients")
        ).otherwise(lit(0))
    ).withColumn(
        "retention_score",
        (col("retention_rate") / TARGETS["patient_retention_target_pct"]) * 100
    ).withColumn(
        "no_show_score",
        greatest(lit(0), lit(100) - ((col("no_show_rate") / TARGETS["no_show_target_pct"]) * 100 - 100))
    ).withColumn(
        "engagement_score",
        greatest(lit(0), lit(100) - (col("cancellation_rate") * 5))
    ).withColumn(
        "patient_health_score",
        least(lit(100), (col("retention_score") * 0.4 + col("no_show_score") * 0.3 + col("engagement_score") * 0.3))
    )
    
    return patient_score

# Create view
try:
    patient_score_df = create_vw_patient_score()
    patient_score_df.createOrReplaceTempView("vw_patient_score")
    patient_score_df.write.format("delta").mode("overwrite").saveAsTable("vw_patient_score")
    print("✅ Created vw_patient_score")
except Exception as e:
    print(f"⚠️ Could not create vw_patient_score: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## General Health Score - Combined View

# CELL ********************

def create_vw_general_health_score():
    """
    General Health Score: Aggregates all 10 health metrics.
    Equal weighting of 10% each.
    """
    
    # Get latest scores from each view
    yoy = spark.table("vw_yoy_health").orderBy(desc("year"), desc("month")).limit(1) \
        .select(col("yoy_health_score").alias("yoy_score"))
    
    financial = spark.table("vw_financial_health") \
        .select(col("financial_health_score").alias("financial_score"))
    
    operational = spark.table("vw_operational_health") \
        .select(col("operational_health_score").alias("operational_score"))
    
    performance = spark.table("vw_performance_target").orderBy(desc("year"), desc("month")).limit(1) \
        .select(col("performance_target_score").alias("performance_score"))
    
    pnl = spark.table("vw_pnl_health").orderBy(desc("year"), desc("month")).limit(1) \
        .select(col("pnl_health_score").alias("pnl_score"))
    
    arap = spark.table("vw_arap_health") \
        .select(col("arap_health_score").alias("arap_score"))
    
    cashflow = spark.table("vw_cashflow_health").orderBy(desc("year"), desc("month")).limit(1) \
        .select(col("cashflow_health_score").alias("cashflow_score"))
    
    inventory = spark.table("vw_inventory_score") \
        .select(col("inventory_health_score").alias("inventory_score"))
    
    claims = spark.table("vw_claims_score").orderBy(desc("year"), desc("month")).limit(1) \
        .select(col("claims_health_score").alias("claims_score"))
    
    patient = spark.table("vw_patient_score").orderBy(desc("year"), desc("month")).limit(1) \
        .select(col("patient_health_score").alias("patient_score"))
    
    # Cross join all scores (single rows each)
    general_health = yoy.crossJoin(financial).crossJoin(operational) \
        .crossJoin(performance).crossJoin(pnl).crossJoin(arap) \
        .crossJoin(cashflow).crossJoin(inventory).crossJoin(claims).crossJoin(patient)
    
    # Calculate weighted average
    general_health = general_health.withColumn(
        "general_health_score",
        (col("yoy_score") * WEIGHTS["yoy_health"] +
         col("financial_score") * WEIGHTS["financial_health"] +
         col("operational_score") * WEIGHTS["operational_health"] +
         col("performance_score") * WEIGHTS["performance_target"] +
         col("pnl_score") * WEIGHTS["pnl_health"] +
         col("arap_score") * WEIGHTS["arap_health"] +
         col("cashflow_score") * WEIGHTS["cashflow_health"] +
         col("inventory_score") * WEIGHTS["inventory_score"] +
         col("claims_score") * WEIGHTS["claims_score"] +
         col("patient_score") * WEIGHTS["patient_score"])
    ).withColumn(
        "health_status",
        when(col("general_health_score") >= 80, "Excellent")
        .when(col("general_health_score") >= 60, "Good")
        .when(col("general_health_score") >= 40, "Fair")
        .otherwise("Needs Attention")
    ).withColumn(
        "calculated_at",
        current_timestamp()
    )
    
    return general_health

# Create view
try:
    general_health_df = create_vw_general_health_score()
    general_health_df.createOrReplaceTempView("vw_general_health_score")
    general_health_df.write.format("delta").mode("overwrite").saveAsTable("vw_general_health_score")
    print("✅ Created vw_general_health_score")
except Exception as e:
    print(f"⚠️ Could not create vw_general_health_score: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary

# CELL ********************

# Display summary of created health views
print("\n" + "="*60)
print("HEALTH SCORE VIEWS CREATION SUMMARY")
print("="*60)

health_views = [
    "vw_yoy_health",
    "vw_financial_health",
    "vw_operational_health",
    "vw_performance_target",
    "vw_pnl_health",
    "vw_arap_health",
    "vw_cashflow_health",
    "vw_inventory_score",
    "vw_claims_score",
    "vw_patient_score",
    "vw_general_health_score"
]

for view_name in health_views:
    try:
        count = spark.table(view_name).count()
        print(f"✅ {view_name}: Ready")
    except:
        print(f"⏳ {view_name}: Pending (waiting for fact tables)")

print("="*60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
