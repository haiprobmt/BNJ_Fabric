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

# # Gold Aggregation: `agg_cashflow` (Single Table)


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260204115600
job_id = '4298'
src_catalog = "gold"
job_group_name = "gold_agg"
src_table = ""


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
SILVER_SCHEMA_PLATO = "`WS-ETL-BNJ`.lh_bnj_silver.plato"
SILVER_SCHEMA_XERO = "`WS-ETL-BNJ`.lh_bnj_silver.xero"
GOLD_SCHEMA = "`WS-ETL-BNJ`.lh_bnj_gold.gold"


SILVER_TABLES = {"xero_invoices": f"{SILVER_SCHEMA_XERO}.silver_invoices"}
GOLD_FACTS = {"cashflow": "lh_bnj_gold.gold.fact_cashflow", "invoice": "lh_bnj_gold.gold.fact_invoice", "ar_aging": "lh_bnj_gold.gold.fact_ar_aging", "ap_aging": "lh_bnj_gold.gold.fact_ap_aging", "inventory": "lh_bnj_gold.gold.fact_inventory"}
AGG_TABLES = {"cashflow": f"{GOLD_SCHEMA}.agg_cashflow"}
GOLD_DIMENSIONS = {"date": "lh_bnj_gold.gold.dim_date"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_agg_cashflow():
    """
    Create agg_cashflow aggregate table for Cashflow Health dashboard.
    
    Target Schema:
    - Cash Conversion Cycle (CCC) = DSO + DIO - DPO
    - AR metrics (Days Sales Outstanding, AR amount, AR score)
    - AP metrics (Days Payable Outstanding, AP score)
    - Inventory metrics (Days Inventory Outstanding, Inventory score)
    - Operating Cash Flow metrics
    - Debt/Interest metrics
    - Overall Cashflow Health Score
    
    Data Sources:
    - fact_cashflow: Operating cash flow
    - xero_invoices: AR metrics (DSO)
    - agg_inventory: Inventory metrics (DIO)
    """
    
    # Read data sources
    fact_cashflow = spark.table(GOLD_FACTS["cashflow"])
    xero_invoices = spark.table(SILVER_TABLES["xero_invoices"])
    
    # Try to read inventory data if available
    try:
        agg_inventory = spark.table("lh_bnj_gold.gold.agg_inventory")
        has_inventory = True
    except:
        has_inventory = False
        print("Warning: agg_inventory not available, using defaults for inventory metrics")
    
    # ========== OPERATING CASH FLOW (from fact_cashflow) ==========
    # Aggregate by date
    ocf_daily = fact_cashflow.groupBy("date_key") \
        .agg(
            sum(when(col("cashflow_category") == "Operating - Receipts", col("cash_in")).otherwise(lit(0))).cast(DecimalType(18, 2)).alias("operating_cash_in"),
            sum(when(col("cashflow_category") == "Operating - Payments", col("cash_out")).otherwise(lit(0))).cast(DecimalType(18, 2)).alias("operating_cash_out"),
            first("bank_account_id").alias("location_key_temp")
        )
    
    ocf_daily = ocf_daily.withColumn(
        "operating_cash_flow",
        (col("operating_cash_in") - col("operating_cash_out")).cast(DecimalType(18, 2))
    )
    
    # ========== AR METRICS (from xero_invoices) ==========
    # DSO = (Average AR / Total Credit Sales) * Days in Period
    # For simplicity, calculate AR outstanding and estimate DSO
    ar_metrics = xero_invoices \
        .filter(col("type") == "ACCREC") \
        .groupBy(get_date_key(to_date(col("invoice_date"))).alias("date_key")) \
        .agg(
            sum("amount_due").cast(DecimalType(18, 2)).alias("total_ar_amount"),
            sum("total").cast(DecimalType(18, 2)).alias("total_sales"),
            avg(datediff(
                coalesce(to_date(col("due_date")), current_date()),
                to_date(col("invoice_date"))
            )).cast(DecimalType(18, 2)).alias("avg_payment_terms")
        )
    
    # Calculate DSO (simplified: AR / daily sales * 30)
    ar_metrics = ar_metrics.withColumn(
        "days_sales_outstanding",
        when(col("total_sales") > 0,
             (col("total_ar_amount") / col("total_sales") * 30)
        ).otherwise(lit(0)).cast(DecimalType(18, 2))
    )
    
    # ========== AP METRICS (estimate from payments to suppliers) ==========
    ap_metrics = fact_cashflow \
        .filter(col("is_supplier") == True) \
        .groupBy("date_key") \
        .agg(
            sum("cash_out").cast(DecimalType(18, 2)).alias("total_ap_payments"),
            count("*").alias("ap_payment_count")
        )
    
    # DPO estimation (assume 30-day terms as baseline, adjust based on payment patterns)
    ap_metrics = ap_metrics.withColumn(
        "days_payable_outstanding",
        lit(30).cast(FloatType())  # Default 30 days, would need AP aging for accuracy
    )
    
    # Current liabilities estimate (monthly AP payments as proxy)
    window_30d = Window.orderBy("date_key").rowsBetween(-29, 0)
    ap_metrics = ap_metrics.withColumn(
        "current_liabilities",
        sum("total_ap_payments").over(window_30d).cast(FloatType())
    )
    
    # ========== DEBT/INTEREST METRICS (from financing transactions) ==========
    debt_metrics = fact_cashflow \
        .filter(col("cashflow_category").contains("Financing")) \
        .groupBy("date_key") \
        .agg(
            sum(when(col("reference").contains("interest") | col("reference").contains("Interest"), col("cash_out"))
                .otherwise(lit(0))).cast(FloatType()).alias("interest_expense"),
            sum(when(~(col("reference").contains("interest") | col("reference").contains("Interest")), col("cash_out"))
                .otherwise(lit(0))).cast(FloatType()).alias("principal_repayment")
        )
    
    debt_metrics = debt_metrics.withColumn(
        "total_debt_financing_expense",
        (col("interest_expense") + col("principal_repayment")).cast(FloatType())
    )
    
    # ========== JOIN ALL METRICS ==========
    agg_cashflow = ocf_daily \
        .join(ar_metrics, "date_key", "left") \
        .join(ap_metrics, "date_key", "left") \
        .join(debt_metrics, "date_key", "left")
    
    # ========== INVENTORY METRICS ==========
    if has_inventory:
        inv_metrics = agg_inventory.select(
            col("invoice_date_key").alias("date_key"),
            col("days_inventory_outstanding"),
            col("inventory_key").alias("inventory_id"),
            col("inventory_health_score").alias("inventory_score_raw")
        )
        agg_cashflow = agg_cashflow.join(inv_metrics, "date_key", "left")
    else:
        agg_cashflow = agg_cashflow \
            .withColumn("days_inventory_outstanding", lit(0).cast(DecimalType(18, 2))) \
            .withColumn("inventory_id", lit(None).cast(LongType())) \
            .withColumn("inventory_score_raw", lit(50).cast(FloatType()))
    
    # ========== CASH CONVERSION CYCLE ==========
    # CCC = DSO + DIO - DPO
    agg_cashflow = agg_cashflow.withColumn(
        "cash_conversion_cycle_days",
        (
            coalesce(col("days_sales_outstanding"), lit(0)) +
            coalesce(col("days_inventory_outstanding"), lit(0)) -
            coalesce(col("days_payable_outstanding"), lit(0))
        ).cast(FloatType())
    )
    
    # ========== OPERATING CASH FLOW RATIO ==========
    # OCF Ratio = Operating Cash Flow / Current Liabilities
    agg_cashflow = agg_cashflow.withColumn(
        "operating_cash_flow_ratio",
        when(col("current_liabilities") > 0,
             col("operating_cash_flow") / col("current_liabilities")
        ).otherwise(lit(0)).cast(FloatType())
    )
    
    # ========== HEALTH SCORES (0-100) ==========
    
    # AR Score: Lower DSO = better (100 if DSO <= 30, 0 if DSO >= 90)
    agg_cashflow = agg_cashflow.withColumn(
        "ar_score",
        greatest(lit(0), least(lit(100), 
            lit(100) - (coalesce(col("days_sales_outstanding"), lit(30)) - 30) * (100/60)
        )).cast(DecimalType(18, 2))
    )
    
    # AP Score: Higher DPO = better (but not too high - optimal around 30-45 days)
    agg_cashflow = agg_cashflow.withColumn(
        "ap_score",
        when(col("days_payable_outstanding").between(30, 45), lit(100))
        .when(col("days_payable_outstanding") < 30, col("days_payable_outstanding") / 30 * 100)
        .when(col("days_payable_outstanding") > 45, greatest(lit(0), lit(100) - (col("days_payable_outstanding") - 45) * 2))
        .otherwise(lit(50)).cast(FloatType())
    )
    
    # Inventory Score (from agg_inventory or calculated)
    agg_cashflow = agg_cashflow.withColumn(
        "inventory_score",
        coalesce(col("inventory_score_raw"), lit(50)).cast(DecimalType(18, 2))
    )
    
    # Interest Score: Lower interest expense relative to OCF = better
    agg_cashflow = agg_cashflow.withColumn(
        "interest_score",
        when(col("operating_cash_flow") > 0,
             greatest(lit(0), least(lit(100), 
                 lit(100) - (coalesce(col("interest_expense"), lit(0)) / col("operating_cash_flow") * 100)
             ))
        ).otherwise(lit(50)).cast(FloatType())
    )
    
    # Principal Score: Ability to cover principal payments
    agg_cashflow = agg_cashflow.withColumn(
        "principal_score",
        when(col("operating_cash_flow") > coalesce(col("principal_repayment"), lit(0)), lit(100))
        .when(col("operating_cash_flow") > 0, 
              col("operating_cash_flow") / greatest(col("principal_repayment"), lit(1)) * 100)
        .otherwise(lit(0)).cast(FloatType())
    )
    
    # Overall Cashflow Health Score (weighted average)
    agg_cashflow = agg_cashflow.withColumn(
        "cashflow_health_score",
        (
            coalesce(col("ar_score"), lit(50)) * 0.25 +
            coalesce(col("ap_score"), lit(50)) * 0.20 +
            coalesce(col("inventory_score"), lit(50)) * 0.20 +
            coalesce(col("interest_score"), lit(50)) * 0.15 +
            coalesce(col("principal_score"), lit(50)) * 0.20
        ).cast(FloatType())
    )
    
    # ========== ADD METADATA ==========
    agg_cashflow = agg_cashflow \
        .withColumn("invoice_date_key", col("date_key").cast(IntegerType())) \
        .withColumn("invoice_id", lit(None).cast(StringType())) \
        .withColumn("location_key", coalesce(col("location_key_temp"), lit(1)).cast(LongType())) \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # ========== SELECT FINAL COLUMNS (matching target schema) ==========
    result = agg_cashflow.select(
        col("ap_score").cast(FloatType()),
        col("ar_score").cast(DecimalType(18, 2)),
        col("cash_conversion_cycle_days").cast(FloatType()),
        col("cashflow_health_score").cast(FloatType()),
        col("current_liabilities").cast(FloatType()),
        col("days_inventory_outstanding").cast(DecimalType(18, 2)),
        col("days_payable_outstanding").cast(FloatType()),
        col("days_sales_outstanding").cast(DecimalType(18, 2)),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("interest_expense").cast(FloatType()),
        col("interest_score").cast(FloatType()),
        col("inventory_id").cast(LongType()),
        col("inventory_score").cast(DecimalType(18, 2)),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("location_key").cast(LongType()),
        col("operating_cash_flow").cast(DecimalType(18, 2)),
        col("operating_cash_flow_ratio").cast(FloatType()),
        col("principal_repayment").cast(FloatType()),
        col("principal_score").cast(FloatType()),
        col("total_ar_amount").cast(DecimalType(18, 2)),
        col("total_debt_financing_expense").cast(FloatType())
    )
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# start_job_instance(batch_id, job_id, msg="Start agg_cashflow")
try:
    df = create_agg_cashflow()
    
    src_cnt = spark.table(AGG_TABLES["cashflow"]).count()
    tgt_cnt = df.count()
    # log_data_quality("agg_cashflow", df, "cashflow_key")

    tgt_table = AGG_TABLES["cashflow"]

    # Ensure table exists before overwrite
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_table}
    USING DELTA
    """)

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tgt_table)
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created agg_cashflow", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print(f"✅ Created {tgt_table} with {src_cnt} rows")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed agg_cashflow. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # LEGACY LOGIC

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
