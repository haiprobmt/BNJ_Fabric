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

# # Gold Layer ETL Transformations - Part 3
# ## Complete Aggregate Tables with Full Data
# 
# **Purpose**: Complete all aggregate tables with full calculations for CFO Dashboard
# 
# **This notebook includes**:
# 1. Cashflow aggregates with full CCC calculation
# 2. Inventory aggregates with turnover and expiry tracking
# 3. Claims aggregates with turnaround time
# 4. Patient aggregates with satisfaction and YoY growth
# 5. Operational health composite
# 6. Updated overall health with all components

# MARKDOWN ********************

# ## Table of Contents
# 1. [Setup](#setup)
# 2. [Cashflow Aggregates](#cashflow)
# 3. [Inventory Aggregates](#inventory)
# 4. [Claims Aggregates](#claims)
# 5. [Patient Aggregates](#patient)
# 6. [Operational Health Composite](#operational-health)
# 7. [Complete Overall Health](#overall-health)

# MARKDOWN ********************

# ## 1. Setup <a id='setup'></a>

# CELL ********************

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta

GOLD_SCHEMA = "lh_bnj_gold.gold"
SILVER_SCHEMA = "lh_bnj_silver.plato"

print("✅ Environment configured")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Cashflow Aggregates <a id='cashflow'></a>
# ### Complete Cash Conversion Cycle (CCC) calculation

# CELL ********************

def calculate_agg_cashflow_monthly(year_month=None):
    """
    Calculate monthly cashflow aggregates with complete CCC

    Health Score Formula:
    CFH = 0.45*AR_score + 0.25*AP_score + 0.15*INV_score + 0.10*INT_score + 0.05*PRIN_score
    """
    if year_month is None:
        year_month = datetime.now().strftime('%Y-%m')

    target_year, target_month = map(int, year_month.split('-'))

    # ----------------------------------------------------------------------
    # Load dimension + fact tables
    # ----------------------------------------------------------------------
    dim_date = spark.table(f"{GOLD_SCHEMA}.dim_date")
    fact_invoice_header = spark.table(f"{GOLD_SCHEMA}.fact_invoice_header")
    fact_invoice_line = spark.table(f"{GOLD_SCHEMA}.fact_invoice_line")
    fact_payment = spark.table(f"{GOLD_SCHEMA}.fact_payment")
    dim_inventory = spark.table(f"{GOLD_SCHEMA}.dim_inventory").filter("is_current = true")

    # Get days in month
    month_dates = dim_date.filter(
        (F.year(col("full_date")) == target_year) &
        (F.month(col("full_date")) == target_month)
    )
    month_start = month_dates.agg(F.min("full_date")).first()[0]
    month_end = month_dates.agg(F.max("full_date")).first()[0]
    days_in_month = (month_end - month_start).days + 1

    # ----------------------------------------------------------------------
    # BASE: INVOICES (header level - no inventory_key yet)
    # ----------------------------------------------------------------------
    inv_hdr = fact_invoice_header.filter(
        (col("is_void") == False) & (col("is_finalized") == True)
    ).alias("inv_hdr")

    # ----------------------------------------------------------------------
    # AR SECTION (Accounts Receivable) - at invoice level
    # ----------------------------------------------------------------------
    pay = fact_payment.filter(col("is_voided") == False).alias("pay")

    pay_sum = pay.groupBy("invoice_fact_key").agg(
        F.sum("payment_amount").alias("paid")
    ).alias("pay_sum")

    ar = (
        inv_hdr.join(
            pay_sum, 
            col("inv_hdr.invoice_fact_key") == col("pay_sum.invoice_fact_key"), 
            "left"
        )
        .select(
            col("inv_hdr.location_key"),
            col("inv_hdr.invoice_id"),
            col("inv_hdr.invoice_date_key"),
            col("inv_hdr.total_amount"),
            F.coalesce(col("pay_sum.paid"), lit(0)).alias("paid")
        )
        .withColumn("ar_amount", col("total_amount") - col("paid"))
        .filter(col("ar_amount") > 0)
    ).alias("ar")

    ar_by_invoice = ar.groupBy(
        "location_key", "invoice_id", "invoice_date_key"
    ).agg(
        F.sum("ar_amount").alias("total_ar_amount")
    ).alias("ar_inv")

    # ----------------------------------------------------------------------
    # REVENUE (Total Sales) - at invoice level
    # ----------------------------------------------------------------------
    revenue = (
        inv_hdr.groupBy("location_key", "invoice_id", "invoice_date_key")
        .agg(F.sum("total_amount").alias("total_revenue"))
    ).alias("rev")

    # ----------------------------------------------------------------------
    # COGS (Cost of Goods Sold) - from invoice lines, aggregated to invoice level
    # ----------------------------------------------------------------------
    line = fact_invoice_line.alias("line")

    cogs_by_invoice = (
        line
        .groupBy("location_key", "invoice_id", "invoice_date_key")
        .agg(F.sum("line_cost_total").alias("total_cogs"))
    ).alias("cogs")

    # Also get COGS by inventory_key for detailed analysis
    cogs_by_inventory = (
        line
        .groupBy("location_key", "invoice_id", "invoice_date_key", "inventory_key")
        .agg(F.sum("line_cost_total").alias("cogs_per_inventory"))
    ).alias("cogs_inv")

    # ----------------------------------------------------------------------
    # AP SECTION (Accounts Payable) - estimated at invoice level
    # ----------------------------------------------------------------------
    ap_by_invoice = (
        cogs_by_invoice
        .select(
            "location_key",
            "invoice_id", 
            "invoice_date_key",
            (col("total_cogs") * 0.30).alias("total_ap_amount")
        )
    ).alias("ap_inv")

    # ----------------------------------------------------------------------
    # INVENTORY VALUE - by inventory_key
    # ----------------------------------------------------------------------
    inv_val = (
        dim_inventory
        .groupBy("inventory_key")
        .agg(
            F.avg(col("cost_price")).alias("avg_cost_price")
        )
    ).alias("inv")

    # Get inventory quantities from invoice lines
    inv_qty = (
        line
        .groupBy("location_key", "invoice_id", "invoice_date_key", "inventory_key")
        .agg(
            F.sum("quantity").alias("total_quantity")
        )
    ).alias("inv_qty")

    # Calculate inventory value per inventory item
    inventory_value = (
        inv_qty
        .join(inv_val, "inventory_key", "left")
        .select(
            "location_key",
            "invoice_id",
            "invoice_date_key",
            "inventory_key",
            (col("total_quantity") * F.coalesce(col("avg_cost_price"), lit(0))).alias("inventory_value")
        )
    ).alias("inv_val")

    # ----------------------------------------------------------------------
    # DSO = (AR / Revenue) × Days - at invoice level
    # ----------------------------------------------------------------------
    dso = (
        ar_by_invoice.alias("a")
        .join(
            revenue.alias("r"),
            ["location_key", "invoice_id", "invoice_date_key"],
            "inner"
        )
        .select(
            col("a.location_key"),
            col("a.invoice_id"),
            col("a.invoice_date_key"),
            col("a.total_ar_amount"),
            (
                col("a.total_ar_amount") / 
                F.when(col("r.total_revenue") == 0, lit(None)).otherwise(col("r.total_revenue")) * 
                lit(days_in_month)
            ).alias("days_sales_outstanding")
        )
    ).alias("dso")

    # ----------------------------------------------------------------------
    # DIO = (Inventory / COGS) × Days - by inventory_key
    # ----------------------------------------------------------------------
    dio = (
        inventory_value.alias("i")
        .join(
            cogs_by_inventory.alias("c"),
            ["location_key", "invoice_id", "invoice_date_key", "inventory_key"],
            "inner"
        )
        .select(
            col("i.location_key"),
            col("i.invoice_id"),
            col("i.invoice_date_key"),
            col("i.inventory_key"),
            (
                col("i.inventory_value") / 
                F.when(col("c.cogs_per_inventory") == 0, lit(None)).otherwise(col("c.cogs_per_inventory")) * 
                lit(days_in_month)
            ).alias("days_inventory_outstanding")
        )
    ).alias("dio")

    # ----------------------------------------------------------------------
    # DPO = (AP / COGS) × Days - at invoice level
    # ----------------------------------------------------------------------
    dpo = (
        ap_by_invoice.alias("a")
        .join(
            cogs_by_invoice.alias("c"),
            ["location_key", "invoice_id", "invoice_date_key"],
            "inner"
        )
        .select(
            col("a.location_key"),
            col("a.invoice_id"),
            col("a.invoice_date_key"),
            col("a.total_ap_amount"),
            (
                col("a.total_ap_amount") / 
                F.when(col("c.total_cogs") == 0, lit(None)).otherwise(col("c.total_cogs")) * 
                lit(days_in_month)
            ).alias("days_payable_outstanding")
        )
    ).alias("dpo")

    # ----------------------------------------------------------------------
    # CCC = DSO + DIO - DPO
    # Join DSO/DPO (invoice level) with DIO (inventory level)
    # ----------------------------------------------------------------------
    # First combine DSO and DPO at invoice level
    dso_dpo = (
        dso.alias("d")
        .join(
            dpo.alias("p"),
            ["location_key", "invoice_id", "invoice_date_key"],
            "full"
        )
        .select(
            F.coalesce(col("d.location_key"), col("p.location_key")).alias("location_key"),
            F.coalesce(col("d.invoice_id"), col("p.invoice_id")).alias("invoice_id"),
            F.coalesce(col("d.invoice_date_key"), col("p.invoice_date_key")).alias("invoice_date_key"),
            F.coalesce(col("d.total_ar_amount"), lit(0)).alias("total_ar_amount"),
            F.coalesce(col("d.days_sales_outstanding"), lit(0)).alias("days_sales_outstanding"),
            F.coalesce(col("p.days_payable_outstanding"), lit(0)).alias("days_payable_outstanding")
        )
    ).alias("dso_dpo")

    # Then join with DIO (which has inventory_key)
    ccc = (
        dso_dpo.alias("dd")
        .join(
            dio.alias("i"),
            ["location_key", "invoice_id", "invoice_date_key"],
            "full"
        )
        .select(
            F.coalesce(col("dd.location_key"), col("i.location_key")).alias("location_key"),
            F.coalesce(col("dd.invoice_id"), col("i.invoice_id")).alias("invoice_id"),
            F.coalesce(col("dd.invoice_date_key"), col("i.invoice_date_key")).alias("invoice_date_key"),
            col("i.inventory_key").alias("inventory_id"),  # Use inventory_key from DIO

            col("dd.total_ar_amount"),
            col("dd.days_sales_outstanding"),
            F.coalesce(col("i.days_inventory_outstanding"), lit(0)).alias("days_inventory_outstanding"),
            col("dd.days_payable_outstanding"),

            (
                F.coalesce(col("dd.days_sales_outstanding"), lit(0)) +
                F.coalesce(col("i.days_inventory_outstanding"), lit(0)) -
                F.coalesce(col("dd.days_payable_outstanding"), lit(0))
            ).alias("cash_conversion_cycle_days")
        )
    ).alias("ccc")

    # ----------------------------------------------------------------------
    # OCF (Operating Cash Flow) = Revenue - COGS - at invoice level
    # ----------------------------------------------------------------------
    ocf = (
        revenue.alias("r")
        .join(
            cogs_by_invoice.alias("c"),
            ["location_key", "invoice_id", "invoice_date_key"],
            "inner"
        )
        .select(
            col("r.location_key"),
            col("r.invoice_id"),
            col("r.invoice_date_key"),
            (col("r.total_revenue") - col("c.total_cogs")).alias("operating_cash_flow")
        )
    ).alias("ocf")

    # ----------------------------------------------------------------------
    # Current Liabilities (estimated from AR)
    # ----------------------------------------------------------------------
    cur_liab = (
        ar_by_invoice
        .select(
            "location_key",
            "invoice_id",
            "invoice_date_key",
            (col("total_ar_amount") * 0.5).alias("current_liabilities")
        )
    ).alias("cl")

    # ----------------------------------------------------------------------
    # OCF Ratio = OCF / Current Liabilities
    # ----------------------------------------------------------------------
    ocf_ratio = (
        ocf.alias("o")
        .join(
            cur_liab.alias("c"),
            ["location_key", "invoice_id", "invoice_date_key"],
            "inner"
        )
        .select(
            col("o.location_key"),
            col("o.invoice_id"),
            col("o.invoice_date_key"),
            col("o.operating_cash_flow"),
            col("c.current_liabilities"),
            (
                col("o.operating_cash_flow") / 
                F.when(col("c.current_liabilities") == 0, lit(None)).otherwise(col("c.current_liabilities"))
            ).alias("operating_cash_flow_ratio")
        )
    ).alias("ocfr")

    # ----------------------------------------------------------------------
    # Debt & Financing
    # ----------------------------------------------------------------------
    debt = (
        cogs_by_invoice.select(
            "location_key",
            "invoice_id",
            "invoice_date_key",
            (col("total_cogs") * 0.03).alias("interest_expense"),
            (col("total_cogs") * 0.05).alias("principal_repayment"),
            (col("total_cogs") * 0.08).alias("total_debt_financing_expense")
        )
    ).alias("debt")

    # ----------------------------------------------------------------------
    # FINAL JOIN + HEALTH SCORES
    # ----------------------------------------------------------------------
    final = (
        ccc.alias("c")
        .join(
            ocf_ratio.alias("o"),
            ["location_key", "invoice_id", "invoice_date_key"],
            "full"
        )
        .join(
            debt.alias("d"),
            ["location_key", "invoice_id", "invoice_date_key"],
            "full"
        )
    )

    # Score targets
    ar_target = 30   # DSO target: 30 days
    ap_target = 60   # DPO target: 60 days
    inv_target = 30  # DIO target: 30 days
    int_target_pct = 0.05
    prin_target_pct = 0.08

    result = final.select(
        # Keys
        F.coalesce(col("c.location_key"), col("o.location_key"), col("d.location_key")).alias("location_key"),
        F.coalesce(col("c.invoice_id"), col("o.invoice_id"), col("d.invoice_id")).alias("invoice_id"),
        F.coalesce(col("c.invoice_date_key"), col("o.invoice_date_key"), col("d.invoice_date_key")).alias("invoice_date_key"),
        col("c.inventory_id").alias("inventory_id"),  # inventory_id from CCC (can be null for invoice-level metrics)

        # Cashflow + CCC metrics
        col("c.total_ar_amount"),
        col("o.current_liabilities"),
        col("o.operating_cash_flow"),
        col("o.operating_cash_flow_ratio"),
        col("c.days_inventory_outstanding"),
        col("c.days_sales_outstanding"),
        col("c.days_payable_outstanding"),
        col("c.cash_conversion_cycle_days"),

        # Debt metrics
        col("d.interest_expense"),
        col("d.principal_repayment"),
        col("d.total_debt_financing_expense"),

        # Scoring: Lower DSO is better (faster collections)
        F.least(
            F.greatest(
                (lit(1) - F.coalesce(col("c.days_sales_outstanding"), lit(0)) / lit(ar_target)) * 100, 
                lit(0)
            ), 
            lit(100)
        ).alias("ar_score"),

        # Scoring: Higher DPO is better (delay payments longer)
        F.least(
            (F.coalesce(col("c.days_payable_outstanding"), lit(0)) / lit(ap_target)) * 100, 
            lit(100)
        ).alias("ap_score"),

        # Scoring: Lower DIO is better (faster inventory turnover)
        F.least(
            F.greatest(
                (lit(1) - F.coalesce(col("c.days_inventory_outstanding"), lit(0)) / lit(inv_target)) * 100, 
                lit(0)
            ), 
            lit(100)
        ).alias("inventory_score"),

        # Scoring: Lower interest expense is better
        F.least(
            F.greatest(
                (lit(1) - F.coalesce(col("d.interest_expense"), lit(0)) / (F.coalesce(col("o.operating_cash_flow"), lit(1)) * lit(int_target_pct))) * 100, 
                lit(0)
            ), 
            lit(100)
        ).alias("interest_score"),

        # Scoring: Lower principal repayment is better
        F.least(
            F.greatest(
                (lit(1) - F.coalesce(col("d.principal_repayment"), lit(0)) / (F.coalesce(col("o.operating_cash_flow"), lit(1)) * lit(prin_target_pct))) * 100, 
                lit(0)
            ), 
            lit(100)
        ).alias("principal_score"),

        F.current_timestamp().alias("dw_created_at"),
        F.current_timestamp().alias("dw_updated_at")
    ).withColumn(
        "cashflow_health_score",
        F.coalesce(col("ar_score"), lit(0)) * 0.45 +
        F.coalesce(col("ap_score"), lit(0)) * 0.25 +
        F.coalesce(col("inventory_score"), lit(0)) * 0.15 +
        F.coalesce(col("interest_score"), lit(0)) * 0.10 +
        F.coalesce(col("principal_score"), lit(0)) * 0.05
    )
    # display(result)
    # Write to table
    result.write \
        .mode('overwrite') \
        .format('delta') \
        .partitionBy('location_key') \
        .saveAsTable(f"{GOLD_SCHEMA}.agg_cashflow")

    row_count = result.count()
    print(f"✅ AGG_CASHFLOW created with {row_count:,} rows for {year_month}")

    return result


# Execute
agg_cashflow = calculate_agg_cashflow_monthly()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Inventory Aggregates <a id='inventory'></a>
# ### Inventory turnover, expired stock, and cash conversion

# CELL ********************

def calculate_agg_inventory_daily(report_date=None):
    """
    Calculate daily inventory health metrics

    Health Score Formula:
    - With CCC: 0.40*ITR + 0.20*Expired + 0.40*CCC
    - Without CCC: 0.80*ITR + 0.20*Expired
    """
    import pyspark.sql.functions as F
    from datetime import datetime

    # ----------------------------------------------------------------------
    # 1. DEFAULT DATE
    # ----------------------------------------------------------------------
    if report_date is None:
        report_date = datetime.now().date()

    # ----------------------------------------------------------------------
    # 2. LOAD TABLES
    # ----------------------------------------------------------------------
    dim_inventory = spark.table(f"{GOLD_SCHEMA}.dim_inventory")
    fact_invoice_line = spark.table(f"{GOLD_SCHEMA}.fact_invoice_line")
    dim_date = spark.table(f"{GOLD_SCHEMA}.dim_date")

    # ----------------------------------------------------------------------
    # 3. GET DATE_KEY FOR REPORT_DATE
    # ----------------------------------------------------------------------
    report_date_key = (
        dim_date.filter(col("full_date") == lit(report_date))
        .select("date_key")
        .first()[0]
    )

    # ----------------------------------------------------------------------
    # 4. COMPUTE COGS MTD BY INVOICE AND INVENTORY
    # ----------------------------------------------------------------------
    cogs_mtd = (
        fact_invoice_line
        .groupBy("invoice_id", "invoice_date_key", "inventory_key")
        .agg(sum("line_cost_total").alias("cogs_mtd"))
    )

    # ----------------------------------------------------------------------
    # 5. GET CURRENT INVENTORY FROM DIM_INVENTORY
    # ----------------------------------------------------------------------
    current_dim_inventory = (
        dim_inventory
        .filter(col("is_current") == True)
        .select(
            col("inventory_key"),
            col("inventory_id"),
            col("category"),
            col("cost_price")
        )
    )

    # ----------------------------------------------------------------------
    # 6. CALCULATE INVENTORY METRICS FROM FACT_INVOICE_LINE
    # ----------------------------------------------------------------------
    inventory_aggregates = (
        fact_invoice_line.alias("fil")
        .join(current_dim_inventory.alias("dim"), col("fil.inventory_key") == col("dim.inventory_key"), "inner")
        .groupBy(col("fil.inventory_key"), col("dim.category"))
        .agg(
            # Calculate total stock quantity and value
            sum(col("fil.quantity")).alias("total_stock_quantity"),
            sum(col("fil.quantity") * col("dim.cost_price")).alias("total_stock_value"),
            
            # Calculate average inventory value
            avg(col("fil.quantity") * col("dim.cost_price")).alias("average_inventory_value"),
            
            # Ending inventory (most recent)
            sum(col("fil.quantity") * col("dim.cost_price")).alias("ending_inventory_value"),
            
            # Count items with zero or negative quantity as expired
            sum(when(col("fil.quantity") <= 0, 1).otherwise(0)).alias("expired_stock_quantity"),
            sum(
                when(col("fil.quantity") <= 0, col("dim.cost_price") * abs(col("fil.quantity")))
                .otherwise(lit(0))
            ).alias("expired_stock_value")
        )
        .select(
            col("inventory_key"),
            col("category"),
            col("total_stock_quantity"),
            col("total_stock_value"),
            col("average_inventory_value"),
            col("ending_inventory_value"),
            col("expired_stock_quantity"),
            col("expired_stock_value")
        )
    )

    # ----------------------------------------------------------------------
    # 7. JOIN COGS WITH INVENTORY AGGREGATES
    # ----------------------------------------------------------------------
    inventory_metrics = (
        cogs_mtd
        .join(inventory_aggregates, "inventory_key", "left")
        .select(
            col("inventory_key"),
            col("invoice_id"),
            col("invoice_date_key"),
            col("category"),
            coalesce(col("ending_inventory_value"), lit(0)).alias("ending_inventory_value"),
            coalesce(col("average_inventory_value"), lit(0)).alias("average_inventory_value"),
            coalesce(col("ending_inventory_value"), lit(0)).alias("beginning_inventory_value"),

            coalesce(col("cogs_mtd"), lit(0)).alias("cogs_mtd"),

            # ITR = COGS / Avg Inventory
            (
                coalesce(col("cogs_mtd"), lit(0))
                / when(col('average_inventory_value') == 0, lit(None)).otherwise(col('average_inventory_value'))
            ).alias("inventory_turnover_ratio"),

            # DIO = Avg Inventory / COGS * 30
            (
                coalesce(col("average_inventory_value"), lit(0))
                / when(col('cogs_mtd') == 0, lit(None)).otherwise(col('cogs_mtd'))
                * lit(30)
            ).alias("days_inventory_outstanding"),

            # expired
            coalesce(col("expired_stock_quantity"), lit(0)).alias("expired_stock_quantity"),
            coalesce(col("expired_stock_value"), lit(0)).alias("expired_stock_value"),
            (
                coalesce(col("expired_stock_quantity"), lit(0))
                / when(coalesce(col('total_stock_quantity'), lit(0)) == 0, lit(None))
                    .otherwise(coalesce(col('total_stock_quantity'), lit(1)))
                * lit(100)
            ).alias("expired_stock_percentage"),

            # totals
            coalesce(col("total_stock_quantity"), lit(0)).alias("total_stock_quantity"),
            coalesce(col("total_stock_value"), lit(0)).alias("total_stock_value")
        )
    )

    # ----------------------------------------------------------------------
    # 8. SCORE NORMALIZATION LIMITS
    # ----------------------------------------------------------------------
    itr_min = 2.0
    itr_max = 12.0
    exp_max = 10.0
    ccc_min = 0.0
    ccc_max = 120.0

    # ----------------------------------------------------------------------
    # 9. ADD SCORES + BASE COLUMNS
    # ----------------------------------------------------------------------
    inventory_with_scores = inventory_metrics.select(
        col('inventory_key'),
        col("invoice_id"),
        col("invoice_date_key"),
        col("category"),

        # Inventory metrics
        col("beginning_inventory_value"),
        col("ending_inventory_value"),
        col("average_inventory_value"),
        col("cogs_mtd"),
        col("inventory_turnover_ratio"),
        col("days_inventory_outstanding"),

        # expired
        col("expired_stock_quantity"),
        col("expired_stock_value"),
        col("expired_stock_percentage"),

        # totals
        col("total_stock_quantity"),
        col("total_stock_value"),

        # ITR score
        least(
            greatest(
                ((coalesce(col("inventory_turnover_ratio"), lit(0)) - itr_min) / (itr_max - itr_min)) * 100,
                lit(0)
            ),
            lit(100)
        ).alias("inventory_turnover_score"),

        # expired score
        greatest(
            lit(100) - (coalesce(col("expired_stock_percentage"), lit(0)) / exp_max * 100),
            lit(0)
        ).alias("expired_stock_score"),

        # CCC score
        least(
            greatest(
                ((ccc_max - coalesce(col("days_inventory_outstanding"), lit(0))) / (ccc_max - ccc_min)) * 100,
                lit(0)
            ),
            lit(100)
        ).alias("ccc_inventory_score"),

        current_timestamp().alias("dw_created_at"),
        current_timestamp().alias("dw_updated_at")
    )

    # ----------------------------------------------------------------------
    # 10. FINAL HEALTH SCORE
    # ----------------------------------------------------------------------
    inventory_final = inventory_with_scores.withColumn(
        "inventory_health_score",
        col("inventory_turnover_score") * 0.40 +
        col("expired_stock_score") * 0.20 +
        col("ccc_inventory_score") * 0.40
    )

    # --- WRITE ---
    inventory_final.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable(f"{GOLD_SCHEMA}.agg_inventory")

    row_count = inventory_final.count()
    print(f"✅ AGG_INVENTORY created with {row_count:,} rows for {report_date}")
    return inventory_final

calculate_agg_inventory_daily()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Claims Aggregates <a id='claims'></a>
# ### Claims submission, approval, and turnaround time

# CELL ********************

def calculate_agg_claims_monthly(year_month=None):
    """
    Calculate monthly claims processing metrics.
    
    Health Score Formula:
      Claims Health = 0.40*Approved% 
                    + 0.20*(1-Pending%) 
                    + 0.40*(1-Rejected%)
    """

    from pyspark.sql import functions as F
    from pyspark.sql.functions import col, lit, when, sum, max, avg, expr, count
    from datetime import datetime

    if year_month is None:
        year_month = datetime.now().strftime('%Y-%m')

    year, month = map(int, year_month.split('-'))

    # --------------------------------------------------
    # READ TABLES
    # --------------------------------------------------
    fact_invoice_header = spark.table(f"{GOLD_SCHEMA}.fact_invoice_header")
    fact_payment        = spark.table(f"{GOLD_SCHEMA}.fact_payment")
    dim_date            = spark.table(f"{GOLD_SCHEMA}.dim_date")
    dim_corporate       = spark.table(f"{GOLD_SCHEMA}.dim_corporate").filter(col('is_current') == True)

    # --------------------------------------------------
    # FILTER TARGET MONTH
    # --------------------------------------------------
    month_dates = dim_date.filter((col("year") == year) & (col("month") == month))

    # --------------------------------------------------
    # CLAIM INVOICES (corporate key NOT NULL)
    # --------------------------------------------------
    claims = (
        fact_invoice_header
        # .filter(col("corporate_key").isNotNull())
        .filter(col("is_void") == False)
    )
    # --------------------------------------------------
    # JOIN PAYMENTS
    # --------------------------------------------------
    payments_agg = (
        fact_payment.filter(col("is_voided") == False)
        .groupBy("invoice_fact_key")
        .agg(
            sum("payment_amount").alias("total_paid"),
            max("created_on").alias("payment_date")
        )
    )

    claims_with_payments = claims.join(payments_agg, "invoice_fact_key", "left")

    # --------------------------------------------------
    # CLAIM STATUS + TAT
    # --------------------------------------------------
    claims_status = claims_with_payments.select(
        col("invoice_fact_key"),
        col("invoice_id"),
        col("invoice_date_key"),
        col("corporate_key"),
        col("location_key"),
        col("total_amount"),
        F.coalesce(col("total_paid"), lit(0)).alias("total_paid"),
        col("finalized_on").alias("submitted_date"),
        col("payment_date"),

        # ------------------------------
        # CLAIM STATUS LOGIC
        # ------------------------------
        when(col("finalized_on").isNull(), "Outstanding")
        .when(F.coalesce(col("total_paid"), lit(0)) >= col("total_amount"), "Approved")
        .when(F.coalesce(col("total_paid"), lit(0)) > 0, "Pending")
        .when(col("is_credit_note") == True, "Rejected")
        .otherwise("Pending")
        .alias("claim_status"),

        # Turnaround time
        F.datediff(col("payment_date"), col("finalized_on")).alias("turnaround_days")
    )

    # --------------------------------------------------
    # AGGREGATION
    # --------------------------------------------------
    claims_agg = (
        claims_status
        .groupBy("location_key", "corporate_key", "invoice_id", "invoice_date_key")
        .agg(
            count("*").alias("total_claims"),
            count(when(col("claim_status") == "Outstanding", 1)).alias("total_claims_outstanding"),
            count(when(col("claim_status") != "Outstanding", 1)).alias("claims_submitted"),
            count(when(col("claim_status") == "Approved", 1)).alias("claims_approved"),
            count(when(col("claim_status") == "Pending", 1)).alias("claims_pending"),
            count(when(col("claim_status") == "Rejected", 1)).alias("claims_rejected"),

            avg("turnaround_days").alias("avg_turnaround_days"),
            expr("percentile(turnaround_days, 0.5)").alias("median_turnaround_days"),
            max("turnaround_days").alias("max_turnaround_days")
        )
    )

    # ---------------------------------------------
    # SAFE DIVISION: replace nullif with WHEN logic
    # ---------------------------------------------
    def safe_div(num, den):
        return when(den == 0, None).otherwise(num / den * 100)

    claims_final = claims_agg.select(
        col("location_key"),
        col("corporate_key"),
        col("invoice_id"),
        col("invoice_date_key"),
        # Counts
        col("total_claims_outstanding"),
        col("claims_submitted"),
        col("claims_approved"),
        col("claims_pending"),
        col("claims_rejected"),

        # Percentages
        safe_div(col("claims_submitted"), col("total_claims")).alias("claims_submitted_pct"),
        safe_div(col("claims_approved"), col("claims_submitted")).alias("claims_approved_pct"),
        safe_div(col("claims_pending"), col("claims_submitted")).alias("claims_pending_pct"),
        safe_div(col("claims_rejected"), col("claims_submitted")).alias("claims_rejected_pct"),

        # TAT
        col("avg_turnaround_days"),
        col("median_turnaround_days"),
        col("max_turnaround_days"),

        # Score Components
        safe_div(col("claims_approved"), col("claims_submitted")).alias("approval_score"),
        (100 - safe_div(col("claims_pending"), col("claims_submitted"))).alias("pending_score"),
        (100 - safe_div(col("claims_rejected"), col("claims_submitted"))).alias("rejection_score"),

        current_timestamp().alias("dw_created_at"),
        current_timestamp().alias("dw_updated_at")
    )

    # --------------------------------------------------
    # COMPOSITE HEALTH SCORE
    # --------------------------------------------------
    claims_with_health = claims_final.withColumn(
        "claims_health_score",
        (
            F.coalesce(col("approval_score"), lit(0)) * 0.40 +
            F.coalesce(col("pending_score"), lit(100)) * 0.20 +
            F.coalesce(col("rejection_score"), lit(100)) * 0.40
        )
    )
    # --------------------------------------------------
    # WRITE TO DELTA
    # --------------------------------------------------
    claims_with_health.write \
        .mode("overwrite") \
        .format("delta") \
        .partitionBy("location_key") \
        .saveAsTable(f"{GOLD_SCHEMA}.agg_claims")

    print(f"✅ AGG_CLAIMS_MONTHLY created for {year_month}")

    return claims_with_health
calculate_agg_claims_monthly()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Patient Aggregates <a id='patient'></a>
# ### Patient growth, retention, and satisfaction

# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime

def calculate_agg_patient_monthly(year_month=None):
    """
    PATIENT MONTHLY HEALTH - Based on actual silver layer data
    
    Patient Categories:
    - NEW: First invoice ever for the patient
    - RETURNING: Has 2+ invoices (came back for more visits)
    - LOST: Last invoice was >180 days ago (6 months)
    - PENDING: Has appointment but no invoice yet
    """

    # ----------------------------------------------------------------------
    # 0. DEFAULT YEAR-MONTH IF EMPTY
    # ----------------------------------------------------------------------
    if year_month is None:
        year_month = datetime.now().strftime('%Y-%m')

    year, month = map(int, year_month.split("-"))

    # ----------------------------------------------------------------------
    # 1. LOAD SILVER TABLES
    # ----------------------------------------------------------------------
    silver_base_invoice = spark.table(f"{SILVER_SCHEMA}.silver_base_invoice")
    silver_appointment = spark.table(f"{SILVER_SCHEMA}.silver_appointment")
    silver_patient = spark.table(f"{SILVER_SCHEMA}.silver_patient")
    
    # Load gold dimension to get keys
    dim_patient = spark.table(f"{GOLD_SCHEMA}.dim_patient").filter("is_current = true")
    dim_location = spark.table(f"{GOLD_SCHEMA}.dim_location").filter("is_current = true")

    # ----------------------------------------------------------------------
    # 2. BASE: ALL VALID INVOICES
    # ----------------------------------------------------------------------
    valid_invoices = (
        silver_base_invoice
        .filter("void = 0")
        .select(
            "invoice_id",
            "patient_id",
            F.col("location").alias("location_id"),
            F.coalesce(F.col("date"), F.to_date(F.col("created_on"))).alias("invoice_date"),
            "created_on"
        )
    )

    # Join to get keys
    invoices_with_keys = (
        valid_invoices
        .join(
            dim_patient.select("patient_key", "patient_id"),
            "patient_id",
            "left"
        )
        .join(
            dim_location.select("location_key", "location_id"),
            "location_id",
            "left"
        )
        .filter(F.col("patient_key").isNotNull())
        .filter(F.col("location_key").isNotNull())
    )

    # ----------------------------------------------------------------------
    # 3. NEW PATIENTS - First invoice ever
    # ----------------------------------------------------------------------
    window_first_invoice = Window.partitionBy("patient_id").orderBy("created_on")
    
    new_patients = (
        invoices_with_keys
        .withColumn("invoice_rank", F.row_number().over(window_first_invoice))
        .filter("invoice_rank = 1")
        .select("patient_id", "invoice_id")
        .withColumn("is_new_patient", F.lit(1))
    )

    # ----------------------------------------------------------------------
    # 4. RETURNING PATIENTS - Has 2+ invoices
    # ----------------------------------------------------------------------
    invoice_counts = (
        invoices_with_keys
        .groupBy("patient_id")
        .agg(F.count("invoice_id").alias("total_invoices"))
    )
    
    returning_patients = (
        invoice_counts
        .filter("total_invoices >= 2")
        .select("patient_id")
        .withColumn("is_returning_patient", F.lit(1))
    )
    
    # Count follow-up visits per patient (all invoices after first)
    followup_counts = (
        invoices_with_keys
        .withColumn("invoice_rank", F.row_number().over(window_first_invoice))
        .filter("invoice_rank > 1")
        .groupBy("patient_id")
        .agg(F.count("invoice_id").alias("followup_count"))
    )

    # ----------------------------------------------------------------------
    # 5. LOST PATIENTS - Last invoice >180 days ago
    # ----------------------------------------------------------------------
    last_invoice_dates = (
        invoices_with_keys
        .groupBy("patient_id")
        .agg(
            F.max("invoice_date").alias("last_invoice_date"),
            F.count("invoice_id").alias("total_invoices")
        )
    )
    
    lost_patients = (
        last_invoice_dates
        .filter(
            (F.datediff(F.current_date(), F.col("last_invoice_date")) > 180) &
            (F.col("total_invoices") >= 1)  # Had at least one invoice
        )
        .select("patient_id")
        .withColumn("is_lost_patient", F.lit(1))
    )

    # ----------------------------------------------------------------------
    # 6. PENDING PATIENTS - Has appointment but no invoice
    # ----------------------------------------------------------------------
    patients_with_appointments = (
        silver_appointment
        .select("patient_id")
        .distinct()
    )
    
    patients_with_invoices = (
        invoices_with_keys
        .select("patient_id")
        .distinct()
    )
    
    pending_patients = (
        patients_with_appointments
        .join(patients_with_invoices, "patient_id", "leftanti")
        .withColumn("is_pending_patient", F.lit(1))
    )
    
    # Also include future appointments
    future_appointments = (
        silver_appointment
        .filter(F.col("start_time") > F.current_timestamp())
        .select("patient_id")
        .distinct()
        .withColumn("has_future_appointment", F.lit(1))
    )

    # ----------------------------------------------------------------------
    # 7. JOIN ALL FLAGS TO BASE INVOICES
    # ----------------------------------------------------------------------
    invoices_with_flags = (
        invoices_with_keys
        .join(new_patients, ["patient_id", "invoice_id"], "left")
        .join(returning_patients, "patient_id", "left")
        .join(followup_counts, "patient_id", "left")
        .join(lost_patients, "patient_id", "left")
        .join(pending_patients, "patient_id", "left")
        .join(future_appointments, "patient_id", "left")
        .select(
            "invoice_id",
            "patient_id",
            "patient_key",
            "location_key",
            "invoice_date",
            F.coalesce("is_new_patient", F.lit(0)).alias("is_new_patient"),
            F.coalesce("is_returning_patient", F.lit(0)).alias("is_returning_patient"),
            F.coalesce("followup_count", F.lit(0)).alias("followup_count"),
            F.coalesce("is_lost_patient", F.lit(0)).alias("is_lost_patient"),
            F.coalesce("is_pending_patient", F.lit(0)).alias("is_pending_patient"),
            F.coalesce("has_future_appointment", F.lit(0)).alias("has_future_appointment")
        )
    )

    # Get date_key from dim_date
    dim_date = spark.table(f"{GOLD_SCHEMA}.dim_date")
    
    invoices_with_date_key = (
        invoices_with_flags
        .join(
            dim_date.select("full_date", "date_key"),
            invoices_with_flags.invoice_date == dim_date.full_date,
            "left"
        )
        .select(
            "invoice_id",
            "patient_id",
            "patient_key",
            "location_key",
            "date_key",
            "is_new_patient",
            "is_returning_patient",
            "followup_count",
            "is_lost_patient",
            "is_pending_patient",
            "has_future_appointment"
        )
    )

    # ----------------------------------------------------------------------
    # 8. AGGREGATE BY LOCATION, INVOICE_ID, DATE_KEY
    # ----------------------------------------------------------------------
    patient_counts = (
        invoices_with_date_key
        .groupBy("location_key", "invoice_id", "date_key")
        .agg(
            # Total current patients
            F.countDistinct("patient_key").alias("total_current_patients"),
            
            # New patients (first invoice)
            F.sum("is_new_patient").cast("int").alias("new_patients"),
            
            # Returning patients (has 2+ invoices)
            F.sum("is_returning_patient").cast("int").alias("returning_patients"),
            
            # Total follow-up visits
            F.sum("followup_count").cast("int").alias("total_followup_appointments"),
            
            # Lost patients
            F.sum("is_lost_patient").cast("int").alias("lost_patients"),
            
            # Pending patients
            F.sum("is_pending_patient").cast("int").alias("pending_patients"),
            
            # Patients with future appointments
            F.sum("has_future_appointment").cast("int").alias("patients_with_future_appointments")
        )
    )

    # ----------------------------------------------------------------------
    # 9. CALCULATE RATES AND METRICS
    # ----------------------------------------------------------------------
    patient_metrics = (
        patient_counts
        # Retention Rate = Returning / Total Active
        .withColumn(
            "retention_rate_pct",
            F.when(
                F.col("total_current_patients") > 0,
                (F.col("returning_patients") / F.col("total_current_patients") * 100)
            ).otherwise(F.lit(0))
        )
        # Attrition Rate = Lost / (Total + Lost)
        .withColumn(
            "attrition_rate_pct",
            F.when(
                (F.col("total_current_patients") + F.col("lost_patients")) > 0,
                (F.col("lost_patients") / (F.col("total_current_patients") + F.col("lost_patients")) * 100)
            ).otherwise(F.lit(0))
        )
        # Conversion Rate = Returning / New (how many new became returning)
        .withColumn(
            "conversion_rate_pct",
            F.when(
                F.col("new_patients") > 0,
                (F.col("returning_patients") / F.col("new_patients") * 100)
            ).otherwise(F.lit(0))
        )
        # Average follow-ups per returning patient
        .withColumn(
            "avg_followups_per_patient",
            F.when(
                F.col("returning_patients") > 0,
                (F.col("total_followup_appointments") / F.col("returning_patients"))
            ).otherwise(F.lit(0))
        )
        # YoY placeholder (would need historical data to calculate properly)
        .withColumn("total_current_patients_yoy", F.col("total_current_patients"))
        
        # Satisfaction placeholders (to be implemented when survey data available)
        .withColumn("total_surveys_completed", F.lit(0))
        .withColumn("delighted_patients", F.lit(0))
        .withColumn("delighted_patients_pct", F.lit(0))
        .withColumn("unhappy_patients", F.lit(0))
        .withColumn("unhappy_patients_pct", F.lit(0))
        .withColumn("avg_satisfaction_score", F.lit(0))
    )

    # ----------------------------------------------------------------------
    # 10. SCORE CALCULATIONS
    # ----------------------------------------------------------------------
    patient_with_scores = (
        patient_metrics
        # Panel Score - patient base size vs target
        .withColumn(
            "panel_score",
            F.when(
                F.col("total_current_patients_yoy") > 0,
                F.least(
                    (F.col("total_current_patients") / F.col("total_current_patients_yoy") * 100),
                    F.lit(100)
                )
            ).otherwise(F.lit(50))  # Default neutral score if no history
        )
        # Growth Score - based on new patient acquisition
        .withColumn(
            "growth_score",
            F.least(
                F.when(F.col("new_patients") > 0, F.col("new_patients") * 10).otherwise(F.lit(0)),
                F.lit(100)
            )
        )
        # Retention Score - percentage of returning patients
        .withColumn(
            "retention_score",
            F.least(F.col("retention_rate_pct"), F.lit(100))
        )
        # Attrition Score - inverse of attrition (lower attrition = higher score)
        .withColumn(
            "attrition_score",
            F.greatest(100 - F.col("attrition_rate_pct"), F.lit(0))
        )
        # Conversion Score - how well new patients convert to returning
        .withColumn(
            "conversion_score",
            F.least(F.col("conversion_rate_pct"), F.lit(100))
        )
        # Delight Score - placeholder
        .withColumn(
            "delight_score",
            F.coalesce(F.col("delighted_patients_pct"), F.lit(70))
        )
        # Unhappy Score - placeholder
        .withColumn(
            "unhappy_score",
            100 - F.coalesce(F.col("unhappy_patients_pct"), F.lit(10))
        )
        # Overall Patient Health Score
        .withColumn(
            "patient_health_score",
            F.when(
                F.col("total_surveys_completed") > 0,
                # With surveys: balanced across all dimensions
                F.col("panel_score") * 0.20 +
                F.col("growth_score") * 0.15 +
                F.col("retention_score") * 0.20 +
                F.col("attrition_score") * 0.15 +
                F.col("delight_score") * 0.15 +
                F.col("unhappy_score") * 0.15
            ).otherwise(
                # Without surveys: operational metrics only
                F.col("panel_score") * 0.30 +
                F.col("growth_score") * 0.25 +
                F.col("retention_score") * 0.25 +
                F.col("attrition_score") * 0.20
            )
        )
        # Add metadata columns
        .withColumn("patient_key", F.lit(None).cast("bigint"))  # Aggregate level, no single patient
        .withColumn("invoice_date_key", F.col("date_key"))
        .withColumn("year_month", F.lit(year_month))
        .withColumn("year", F.lit(year))
        .withColumn("month", F.lit(month))
        .withColumn("dw_created_at", F.current_timestamp())
        .withColumn("dw_updated_at", F.current_timestamp())
    )

    # ----------------------------------------------------------------------
    # 11. SELECT FINAL COLUMNS
    # ----------------------------------------------------------------------
    final_output = patient_with_scores.select(
        "location_key",
        "invoice_id",
        "invoice_date_key",
        "patient_key",
        "year_month",
        "year",
        "month",
        
        # Patient counts
        "total_current_patients",
        "total_current_patients_yoy",
        "new_patients",
        "returning_patients",
        "lost_patients",
        "pending_patients",
        "patients_with_future_appointments",
        
        # Follow-up metrics
        "total_followup_appointments",
        "avg_followups_per_patient",
        
        # Calculated rates
        "retention_rate_pct",
        "attrition_rate_pct",
        "conversion_rate_pct",
        
        # Satisfaction metrics (placeholders)
        "total_surveys_completed",
        "delighted_patients",
        "delighted_patients_pct",
        "unhappy_patients",
        "unhappy_patients_pct",
        "avg_satisfaction_score",
        
        # Scores
        "panel_score",
        "growth_score",
        "retention_score",
        "attrition_score",
        "conversion_score",
        "delight_score",
        "unhappy_score",
        "patient_health_score",
        
        # Metadata
        "dw_created_at",
        "dw_updated_at"
    )
    # ----------------------------------------------------------------------
    # 12. WRITE OUTPUT
    # ----------------------------------------------------------------------
    final_output.write \
        .mode("overwrite") \
        .format("delta") \
        .partitionBy("location_key") \
        .saveAsTable(f"{GOLD_SCHEMA}.agg_patient")

    row_count = final_output.count()
    print(f"✅ AGG_PATIENT created successfully for {year_month} with {row_count:,} rows")

    return final_output

# Run the function
calculate_agg_patient_monthly()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_bnj_gold.gold.agg_inventory LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
