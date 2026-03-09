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

# # Create agg_pl Aggregate Table
# 
# This notebook creates the `agg_pl` (Profit & Loss) table following the exact schema from `lh_bnj_gold`.
# 
# ## Target Schema: `lh_bnj_gold.gold.agg_pl`
# 
# | Column | Data Type | Description |
# |--------|-----------|-------------|
# | invoice_id | varchar | Invoice identifier |
# | invoice_date_key | int | FK to dim_date |
# | inventory_key | bigint | FK to dim_inventory |
# | location_key | bigint | FK to dim_location |
# | total_revenue | decimal | Total revenue amount |
# | cost_of_sales | decimal | Cost of goods sold |
# | gross_profit | decimal | Revenue - COGS |
# | operating_expenses | decimal | Operating expenses |
# | operating_profit | decimal | Gross profit - OPEX |
# | gross_profit_margin | real | GP / Revenue (%) |
# | operating_profit_margin | real | OP / Revenue (%) |
# | target_* | decimal | Budget/target values |
# | *_variance | decimal | Actual vs target variance |
# | *_score | real | Performance scores |
# | pl_health_score | real | Overall P&L health |
# | dw_created_at | datetime2 | Record creation timestamp |
# | dw_updated_at | datetime2 | Record update timestamp |


# MARKDOWN ********************

# ## Configuration

# CELL ********************

# Configuration
SILVER_SCHEMA = "lh_bnj_silver.xero"
GOLD_CATALOG = "lh_bnj_gold"
GOLD_SCHEMA = "gold"
TABLE_NAME = "agg_pl"

# Full table path
TARGET_TABLE = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{TABLE_NAME}"
OUTPUT_PATH = "Tables/gold/agg_pl"

print(f"Source: {SILVER_SCHEMA}")
print(f"Target: {TARGET_TABLE}")
print(f"Output Path: {OUTPUT_PATH}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Define Schema

# CELL ********************

from pyspark.sql.types import *

# Define exact schema matching lh_bnj_gold.gold.agg_pl (comprehensive version)
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

print(f"Schema defined with {len(AGG_PL_SCHEMA.fields)} columns")
for field in AGG_PL_SCHEMA.fields:
    print(f"  {field.name}: {field.dataType}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Read Source Data from Silver Layer

# CELL ********************

from pyspark.sql.functions import *

# Configuration: Base currency for P&L reporting
BASE_CURRENCY = "CurrencyCode.SGD"  # Filter to base currency only

# Read invoices from silver layer (ACCREC = Sales/Revenue)
# Filter to base currency to avoid mixing different currency values
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
    FROM {SILVER_SCHEMA}.silver_invoices
    WHERE status IN ('AUTHORISED', 'PAID')
      AND invoice_date IS NOT NULL
      AND currency_code = '{BASE_CURRENCY}'
""")

print(f"Total invoices ({BASE_CURRENCY}): {df_invoices.count()}")
print(f"Sales (ACCREC): {df_invoices.filter(col('type') == 'ACCREC').count()}")
print(f"Purchases (ACCPAY): {df_invoices.filter(col('type') == 'ACCPAY').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Try to read line items for more detailed P&L breakdown
try:
    df_line_items = spark.sql(f"""
        SELECT 
            invoice_id,
            line_item_id,
            account_code,
            description,
            CAST(quantity AS DECIMAL(18,2)) as quantity,
            CAST(unit_amount AS DECIMAL(18,2)) as unit_amount,
            CAST(line_amount AS DECIMAL(18,2)) as line_amount,
            CAST(tax_amount AS DECIMAL(18,2)) as tax_amount
        FROM {SILVER_SCHEMA}.silver_invoice_line_items
    """)
    HAS_LINE_ITEMS = True
    print(f"Line items available: {df_line_items.count()} records")
except Exception as e:
    HAS_LINE_ITEMS = False
    print(f"Line items not available: {e}")
    print("Will use invoice totals for P&L calculation")

# Try to read payments data for account_id (bank account)
# Filter out DELETED payments and only include AUTHORISED payments
try:
    df_payments = spark.sql(f"""
        SELECT 
            invoice_id,
            account_id,
            CAST(amount AS DECIMAL(18,2)) as payment_amount,
            DATE(payment_date) as payment_date,
            status
        FROM {SILVER_SCHEMA}.silver_payments
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
    print("account_id will be NULL")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Calculate P&L Metrics

# CELL ********************

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
    display(df_with_keys)
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

print(f"Revenue records: {df_revenue.count()}")
print(f"Cost records: {df_costs.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

print(f"Combined P&L records: {df_pl.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Add Target/Budget Values and Variances

# CELL ********************

# Add target values (using industry benchmarks as defaults)
# In production, these would come from a budget table

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

print("Target values added")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate variances
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

print("Variances calculated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Calculate Performance Scores

# CELL ********************

# Calculate performance scores (0-100 scale)
# Score logic: 100 = at or better than target, 0 = significantly below target

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
         
print("Performance scores calculated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6: Build Final DataFrame

# CELL ********************

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

print(f"\n=== agg_pl Final ===")
print(f"Total records: {df_agg_pl.count()}")
print(f"\nSchema:")
df_agg_pl.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7: Save to Gold Layer

# CELL ********************

# Option: Create managed table in catalog (uncomment to use)
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{GOLD_SCHEMA}")

df_agg_pl.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"✓ Saved to catalog table: {TARGET_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 8: Verify Data

# CELL ********************

# Read back and verify
df_verify = spark.read.format("delta").load('abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/102823e0-12f1-4ca5-b61b-a2df5d75beb2/Tables/gold_test/agg_pl')

print("=== P&L Summary ===")
df_verify.selectExpr(
    "SUM(total_revenue) as total_revenue",
    "SUM(cost_of_sales) as total_cogs",
    "SUM(gross_profit) as total_gross_profit",
    "SUM(operating_expenses) as total_opex",
    "SUM(operating_profit) as total_operating_profit"
).show(truncate=False)

print("\n=== Margin Averages ===")
df_verify.selectExpr(
    "ROUND(AVG(gross_profit_margin), 2) as avg_gp_margin",
    "ROUND(AVG(operating_profit_margin), 2) as avg_op_margin"
).show()

print("\n=== Performance Scores ===")
df_verify.selectExpr(
    "ROUND(AVG(revenue_score), 2) as avg_revenue_score",
    "ROUND(AVG(cogs_score), 2) as avg_cogs_score",
    "ROUND(AVG(gross_profit_score), 2) as avg_gp_score",
    "ROUND(AVG(opex_score), 2) as avg_opex_score",
    "ROUND(AVG(operating_profit_score), 2) as avg_op_score",
    "ROUND(AVG(pl_health_score), 2) as avg_health_score"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Show sample records
print("=== Sample Records ===")
df_verify.select(
    "invoice_id", 
    "invoice_date_key",
    "total_revenue", 
    "gross_profit",
    "gross_profit_margin",
    "pl_health_score"
).filter(col("total_revenue") > 0).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Diagnostic: Check silver_payments data
print("=== silver_payments Data Check ===")

df_payments_check = spark.sql(f"""
    SELECT 
        COUNT(*) as total_payments,
        COUNT(account_id) as non_null_account_id,
        COUNT(DISTINCT account_id) as unique_accounts,
        COUNT(DISTINCT invoice_id) as unique_invoices
    FROM {SILVER_SCHEMA}.silver_payments
""")
df_payments_check.show()

# Sample some records
print("\n=== Sample Payments with account_id ===")
spark.sql(f"""
    SELECT 
        payment_id,
        invoice_id,
        account_id,
        amount,
        payment_date,
        status
    FROM {SILVER_SCHEMA}.silver_payments
    WHERE account_id IS NOT NULL
    LIMIT 10
""").show(truncate=False)

# Check if account_id is empty string vs NULL
print("\n=== account_id Values Distribution ===")
spark.sql(f"""
    SELECT 
        CASE 
            WHEN account_id IS NULL THEN 'NULL'
            WHEN account_id = '' THEN 'EMPTY_STRING'
            ELSE 'HAS_VALUE'
        END as account_id_status,
        COUNT(*) as cnt
    FROM {SILVER_SCHEMA}.silver_payments
    GROUP BY 1
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary
# 
# Created `agg_pl` table with the following columns:
# 
# | Category | Columns |
# |----------|--------|
# | **Identifiers** | invoice_id, invoice_date_key, inventory_key, location_key |
# | **Actuals** | total_revenue, cost_of_sales, gross_profit, operating_expenses, operating_profit |
# | **Margins** | gross_profit_margin, operating_profit_margin |
# | **Targets** | target_revenue, target_cogs, target_gross_profit, target_opex, target_operating_profit |
# | **Variances** | revenue_variance, revenue_variance_pct, cogs_variance, opex_variance |
# | **Scores** | revenue_score, cogs_score, gross_profit_score, opex_score, operating_profit_score, pl_health_score |
# | **Audit** | dw_created_at, dw_updated_at |
