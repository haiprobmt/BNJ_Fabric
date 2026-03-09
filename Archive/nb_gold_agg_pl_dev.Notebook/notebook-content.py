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

# # Gold Aggregation: `agg_pl` (Single Table)


# CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260205100900
job_id = '7806'
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

SILVER_SCHEMA_XERO = "`WS-ETL-BNJ`.lh_bnj_silver.xero"
GOLD_SCHEMA = "`WS-ETL-BNJ`.lh_bnj_gold.gold"

AGG_TABLES = {"pl": "lh_bnj_gold.gold.agg_pl"}
SILVER_TABLES = {
    "xero_invoices": "lh_bnj_silver.xero.silver_invoices"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#===Step 1: Define Schema===
BASE_CURRENCY = "CurrencyCode.SGD" 
AGG_PL_SCHEMA = StructType([
    # Identifiers
    StructField("invoice_id", StringType(), True),
    StructField("invoice_date_key", IntegerType(), True),
    StructField("inventory_key", LongType(), True),
    StructField("location_key", LongType(), True),
    StructField("account_id", StringType(), True),  # Bank account from payments
    
    # Actual P&L Values (decimal)
    StructField("total_revenue", DecimalType(18, 2), True),
    StructField("cost_of_sales", DecimalType(18, 2), True),
    StructField("gross_profit", DecimalType(18, 2), True),
    StructField("operating_expenses", DecimalType(18, 2), True),
    StructField("operating_profit", DecimalType(18, 2), True),
    
    # Margins (real/float)
    StructField("gross_profit_margin", FloatType(), True),
    StructField("operating_profit_margin", FloatType(), True),
    
    # Target/Budget Values (decimal)
    StructField("target_revenue", DecimalType(18, 2), True),
    StructField("target_cogs", DecimalType(18, 2), True),
    StructField("target_gross_profit", DecimalType(18, 2), True),
    StructField("target_opex", DecimalType(18, 2), True),
    StructField("target_operating_profit", DecimalType(18, 2), True),
    
    # Variances (decimal)
    StructField("revenue_variance", DecimalType(18, 2), True),
    StructField("revenue_variance_pct", FloatType(), True),
    StructField("cogs_variance", DecimalType(18, 2), True),
    StructField("opex_variance", DecimalType(18, 2), True),
    
    # Performance Scores (real/float)
    StructField("revenue_score", FloatType(), True),
    StructField("cogs_score", FloatType(), True),
    StructField("gross_profit_score", FloatType(), True),
    StructField("opex_score", FloatType(), True),
    StructField("operating_profit_score", FloatType(), True),
    StructField("pl_health_score", FloatType(), True),
    
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

def trasform_agg_pl():

    #===Step 2: Read Source Data from Silver Layer===
    try:
        df_payments = spark.sql(f"""
            SELECT 
                invoice_id,
                account_id,
                CAST(amount AS DECIMAL(18,2)) as payment_amount,
                DATE(payment_date) as payment_date,
                status
            FROM {SILVER_SCHEMA_XERO}.silver_payments
            WHERE account_id IS NOT NULL
            AND status = 'AUTHORISED'
        """)
        HAS_PAYMENTS = True
        print(f"Payments available (AUTHORISED only): {df_payments.count()} records")
        
        # Get first/primary account_id per invoice (in case of multiple payments)
        df_invoice_accounts = df_payments.groupBy("invoice_id").agg(
            first("account_id").alias("account_id")
        )
        print(f"Unique invoices with payments: {df_invoice_accounts.count()}")
        
    except Exception as e:
        HAS_PAYMENTS = False
        df_invoice_accounts = None
        print(f"Payments not available: {e}")

        
    df_invoices = spark.sql(f"""
        SELECT 
            invoice_id,
            contact_id,
            contact_name,
            type,
            status,
            currency_code,
            DATE(invoice_date) as invoice_date,
            CAST(subtotal AS DECIMAL(18,2)) as sub_total,
            CAST(total_tax AS DECIMAL(18,2)) as total_tax,
            CAST(total AS DECIMAL(18,2)) as total_amount
        FROM {SILVER_SCHEMA_XERO}.silver_invoices
        WHERE status IN ('AUTHORISED', 'PAID')
        AND invoice_date IS NOT NULL
        AND currency_code = '{BASE_CURRENCY}'
    """)


    #====Step 3: Calculate P&L Metrics====

    # Calculate P&L from invoices
    # Revenue = Sales invoices (ACCREC)
    # COGS/OPEX = Purchase invoices (ACCPAY) - simplified allocation

    # Add date key
    df_with_keys = df_invoices \
        .withColumn("invoice_date_key", date_format(col("invoice_date"), "yyyyMMdd").cast(IntegerType()))
    # Join with payments to get account_id (if available)
    if HAS_PAYMENTS and df_invoice_accounts is not None:
        df_with_keys = df_with_keys.join(
            df_invoice_accounts,
            on="invoice_id",
            how="left"
        )
        print("Joined with payments data for account_id")

    else:
        df_with_keys = df_with_keys.withColumn("account_id", lit(None).cast(StringType()))
        print("No payments data - account_id will be NULL")

    # Separate Revenue and Costs
    df_revenue = df_with_keys.filter(col("type") == "ACCREC") \
        .withColumn("total_revenue", col("total_amount")) \
        .withColumn("cost_of_sales", lit(0).cast(DecimalType(18,2))) \
        .withColumn("operating_expenses", lit(0).cast(DecimalType(18,2)))

    df_costs = df_with_keys.filter(col("type") == "ACCPAY") \
        .withColumn("total_revenue", lit(0).cast(DecimalType(18,2))) \
        .withColumn("cost_of_sales", (col("total_amount") * 0.7).cast(DecimalType(18,2))) \
        .withColumn("operating_expenses", (col("total_amount") * 0.3).cast(DecimalType(18,2)))


    # Combine and calculate derived metrics
    df_combined = df_revenue.union(df_costs.select(df_revenue.columns))

    # Calculate P&L metrics per invoice
    df_pl = df_combined \
        .withColumn("gross_profit", 
            (col("total_revenue") - col("cost_of_sales")).cast(DecimalType(18,2))) \
        .withColumn("operating_profit", 
            (col("total_revenue") - col("cost_of_sales") - col("operating_expenses")).cast(DecimalType(18,2))) \
        .withColumn("gross_profit_margin",
            when(col("total_revenue") > 0, 
                (col("gross_profit") / col("total_revenue") * 100)).otherwise(lit(0)).cast(FloatType())) \
        .withColumn("operating_profit_margin",
            when(col("total_revenue") > 0, 
                (col("operating_profit") / col("total_revenue") * 100)).otherwise(lit(0)).cast(FloatType()))


    #===Step 4: Add Target/Budget Values and Variances===

    # Default targets based on typical margins
    TARGET_GROSS_MARGIN = 0.40  # 40% gross margin target
    TARGET_OPERATING_MARGIN = 0.15  # 15% operating margin target
    TARGET_COGS_RATIO = 0.60  # 60% COGS ratio
    TARGET_OPEX_RATIO = 0.25  # 25% OPEX ratio

    df_with_targets = df_pl \
        .withColumn("target_revenue", col("total_revenue").cast(DecimalType(18,2))) \
        .withColumn("target_cogs", 
            (col("total_revenue") * TARGET_COGS_RATIO).cast(DecimalType(18,2))) \
        .withColumn("target_gross_profit", 
            (col("total_revenue") * TARGET_GROSS_MARGIN).cast(DecimalType(18,2))) \
        .withColumn("target_opex", 
            (col("total_revenue") * TARGET_OPEX_RATIO).cast(DecimalType(18,2))) \
        .withColumn("target_operating_profit", 
            (col("total_revenue") * TARGET_OPERATING_MARGIN).cast(DecimalType(18,2)))



    df_with_variances = df_with_targets \
        .withColumn("revenue_variance", 
            (col("total_revenue") - col("target_revenue")).cast(DecimalType(18,2))) \
        .withColumn("revenue_variance_pct",
            when(col("target_revenue") > 0, 
                (col("revenue_variance") / col("target_revenue") * 100)).otherwise(lit(0)).cast(FloatType())) \
        .withColumn("cogs_variance", 
            (col("target_cogs") - col("cost_of_sales")).cast(DecimalType(18,2))) \
        .withColumn("opex_variance", 
            (col("target_opex") - col("operating_expenses")).cast(DecimalType(18,2)))


    #===Step 5: Calculate Performance Scores===
    df_with_scores = df_with_variances \
    .withColumn("revenue_score",
        when(col("target_revenue") == 0, lit(100))
        .otherwise(least(lit(100), greatest(lit(0), 
            (col("total_revenue") / col("target_revenue") * 100)))).cast(FloatType())) \
    .withColumn("cogs_score",
        when(col("target_cogs") == 0, lit(100))
        .otherwise(least(lit(100), greatest(lit(0), 
            (lit(2) - col("cost_of_sales") / col("target_cogs")) * 50))).cast(FloatType())) \
    .withColumn("gross_profit_score",
        when(col("target_gross_profit") == 0, lit(100))
        .otherwise(least(lit(100), greatest(lit(0), 
            (col("gross_profit") / col("target_gross_profit") * 100)))).cast(FloatType())) \
    .withColumn("opex_score",
        when(col("target_opex") == 0, lit(100))
        .otherwise(least(lit(100), greatest(lit(0), 
            (lit(2) - col("operating_expenses") / col("target_opex")) * 50))).cast(FloatType())) \
    .withColumn("operating_profit_score",
        when(col("target_operating_profit") == 0, lit(100))
        .otherwise(least(lit(100), greatest(lit(0), 
            (col("operating_profit") / col("target_operating_profit") * 100)))).cast(FloatType()))

    # Calculate overall P&L health score (weighted average)
    df_with_scores = df_with_scores \
        .withColumn("pl_health_score",
            ((col("revenue_score") * 0.25) + 
            (col("gross_profit_score") * 0.35) + 
            (col("operating_profit_score") * 0.40)).cast(FloatType()))


    #===Step 6: Build Final DataFrame===
    # Select final columns matching target schema
    df_agg_pl = df_with_scores.select(
        # Identifiers
        col("invoice_id").cast(StringType()).alias("invoice_id"),
        col("invoice_date_key").cast(IntegerType()).alias("invoice_date_key"),
        lit(None).cast(LongType()).alias("inventory_key"),
        lit(None).cast(LongType()).alias("location_key"),
        col("account_id").cast(StringType()).alias("account_id"),  # Bank account from payments
        
        # Actual P&L Values
        col("total_revenue").cast(DecimalType(18,2)).alias("total_revenue"),
        col("cost_of_sales").cast(DecimalType(18,2)).alias("cost_of_sales"),
        col("gross_profit").cast(DecimalType(18,2)).alias("gross_profit"),
        col("operating_expenses").cast(DecimalType(18,2)).alias("operating_expenses"),
        col("operating_profit").cast(DecimalType(18,2)).alias("operating_profit"),
        
        # Margins
        col("gross_profit_margin").cast(FloatType()).alias("gross_profit_margin"),
        col("operating_profit_margin").cast(FloatType()).alias("operating_profit_margin"),
        
        # Target Values
        col("target_revenue").cast(DecimalType(18,2)).alias("target_revenue"),
        col("target_cogs").cast(DecimalType(18,2)).alias("target_cogs"),
        col("target_gross_profit").cast(DecimalType(18,2)).alias("target_gross_profit"),
        col("target_opex").cast(DecimalType(18,2)).alias("target_opex"),
        col("target_operating_profit").cast(DecimalType(18,2)).alias("target_operating_profit"),
        
        # Variances
        col("revenue_variance").cast(DecimalType(18,2)).alias("revenue_variance"),
        col("revenue_variance_pct").cast(FloatType()).alias("revenue_variance_pct"),
        col("cogs_variance").cast(DecimalType(18,2)).alias("cogs_variance"),
        col("opex_variance").cast(DecimalType(18,2)).alias("opex_variance"),
        
        # Scores
        col("revenue_score").cast(FloatType()).alias("revenue_score"),
        col("cogs_score").cast(FloatType()).alias("cogs_score"),
        col("gross_profit_score").cast(FloatType()).alias("gross_profit_score"),
        col("opex_score").cast(FloatType()).alias("opex_score"),
        col("operating_profit_score").cast(FloatType()).alias("operating_profit_score"),
        col("pl_health_score").cast(FloatType()).alias("pl_health_score"),
        
        # Audit columns
        current_timestamp().alias("dw_created_at"),
        current_timestamp().alias("dw_updated_at")
    )

    return df_agg_pl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start agg_pl")
try:
    df = trasform_agg_pl()
    src_cnt = 0
    tgt_cnt = df.count()
    # log_data_quality("agg_pl", df, "pl_key")

    target_table = AGG_TABLES["pl"]

    # Ensure table exists before overwrite
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table}
    USING DELTA
    """)

    #df.write.format("delta").mode("overwrite").saveAsTable(target_table)

    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created agg_pl", src_row_num=src_cnt, tgt_row_num=tgt_cnt)

    print(f"Total records: {tgt_cnt}")
    print(f"\nSchema:", df.printSchema())
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed agg_pl. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
    result = daily_pl \
        .withColumn("invoice_date_key", F.col("date_key")) \
        .withColumn("invoice_id", F.lit(None).cast(StringType())) \
        .withColumn("location_key", F.lit(1).cast(LongType())) \
        .withColumn("inventory_key", F.lit(-1).cast(LongType())) \
        .withColumn("dw_created_at", F.current_timestamp()) \
        .withColumn("dw_updated_at", F.current_timestamp())
    
    # # Select a compact set of columns
    # result = daily_pl.select(
    #     F.col("cost_of_sales").cast(DecimalType(18,2)),
    #     F.col("dw_created_at"),
    #     F.col("dw_updated_at"),
    #     F.col("gross_profit").cast(DecimalType(18,2)),
    #     F.col("gross_profit_margin").cast(FloatType()),
    #     F.col("invoice_date_key").cast(IntegerType()),
    #     F.col("location_key").cast(LongType()),
    #     F.col("pl_health_score").cast(FloatType()),
    #     F.col("total_revenue").cast(DecimalType(18,2))
    # )
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
