# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2aae23b9-c116-40b8-9787-a7c48707bf50",
# META       "default_lakehouse_name": "lh_bnj_metadata",
# META       "default_lakehouse_workspace_id": "87076c77-5525-4288-9ae6-8631261bdbd5",
# META       "known_lakehouses": [
# META         {
# META           "id": "2aae23b9-c116-40b8-9787-a7c48707bf50"
# META         },
# META         {
# META           "id": "de0d3060-7353-4b93-a732-2f9d8bbf09b1"
# META         },
# META         {
# META           "id": "93702e40-ff7a-4104-b9d2-e3f67a68cc5d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold Aggregation: `agg_pl` (Profit & Loss)
# 
# This notebook creates the `agg_pl` (Profit & Loss) aggregate table.
# 
# ## Target Schema: `lh_bnj_gold.gold.agg_pl`
# 
# | Column | Data Type | Description |
# |--------|-----------|-------------|
# | invoice_id | varchar | Invoice identifier |
# | invoice_date_key | int | FK to dim_date |
# | inventory_key | bigint | FK to dim_inventory |
# | location_key | bigint | FK to dim_location |
# | account_id | varchar | Bank account from payments |
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


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260225131600
job_id = '7806'
src_catalog = "gold"
job_group_name = "gold_agg"
src_table = ""
tgt_catalog = "gold"
WORKSPACE_NAME = "WS-ETL-BNJ"

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


# Configuration
SILVER_SCHEMA = f"`{WORKSPACE_NAME}`.lh_bnj_silver.plato"
GOLD_CATALOG = f"`{WORKSPACE_NAME}`.lh_bnj_gold"
GOLD_SCHEMA = tgt_catalog
TABLE_NAME = "agg_pl_daily"

# Full table path
TARGET_TABLE = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{TABLE_NAME}"

# Base currency for P&L reporting
# BASE_CURRENCY = "CurrencyCode.SGD"

# Target ratios (industry benchmarks)
TARGET_GROSS_MARGIN = 0.40   # 40% gross margin target
TARGET_OPERATING_MARGIN = 0.15  # 15% operating margin target
TARGET_COGS_RATIO = 0.60     # 60% COGS ratio
TARGET_OPEX_RATIO = 0.25     # 25% OPEX ratio

AGG_TABLES = {"pl": TARGET_TABLE}

print(f"Source: {SILVER_SCHEMA}")
print(f"Target: {TARGET_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_agg_pl():
    """
    Create agg_pl aggregate table for Profit & Loss Health dashboard.
    Uses invoice-level detail from PLATO silver layer.

    NOTE:
    plato.silver_invoice contains sales invoices only.
    It does not contain Xero-style invoice type (ACCREC / ACCPAY),
    so this version treats all valid invoices as revenue.
    COGS and OPEX are set to 0 in this notebook unless another source is added.
    """

    # Load targets from brz_target table
    targets = get_targets_for_period()
    REVENUE_TARGET = targets["revenue_target"]
    COGS_TARGET = targets["cost_of_goods_sold"]
    OPEX_TARGET = targets["operating_expenses"]
    PROFIT_TARGET = targets["profit"]

    print(f"📊 Loaded targets: Revenue={REVENUE_TARGET:,.0f}, COGS={COGS_TARGET:,.0f}, OPEX={OPEX_TARGET:,.0f}, Profit={PROFIT_TARGET:,.0f}")

    # Step 1: Read invoices from PLATO silver layer
    # Keep only finalized, non-void invoices with invoice_date
    df_invoices = spark.sql(f"""
        SELECT
            CAST(invoice_id AS STRING) as invoice_id,
            CAST(location_id AS BIGINT) as location_id,
            DATE(invoice_date) as invoice_date,
            CAST(subtotal AS DECIMAL(18,2)) as sub_total,
            CAST(tax_amount AS DECIMAL(18,2)) as total_tax,
            CAST(total_amount AS DECIMAL(18,2)) as total_amount,
            status,
            CAST(is_finalized AS BOOLEAN) as is_finalized,
            CAST(is_void AS BOOLEAN) as is_void,
            CAST(is_credit_debit_note AS BOOLEAN) as is_credit_debit_note
        FROM {SILVER_SCHEMA}.silver_invoice
        WHERE invoice_date IS NOT NULL
          AND COALESCE(is_finalized, false) = true
          AND COALESCE(is_void, false) = false
    """)

    print(f"Total valid invoices: {df_invoices.count()}")

    # Step 2: Add keys
    df_with_keys = (
        df_invoices
        .withColumn("invoice_date_key", date_format(col("invoice_date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("account_id", lit(None).cast(StringType()))
    )

    # Step 3: Treat all Plato invoices as revenue
    df_combined = (
        df_with_keys
        .withColumn("total_revenue", col("total_amount"))
        .withColumn("cost_of_sales", lit(0).cast(DecimalType(18,2)))
        .withColumn("operating_expenses", lit(0).cast(DecimalType(18,2)))
    )

    # Step 4: Calculate P&L metrics per invoice
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

    # Step 5: Add target values
    GP_TARGET = REVENUE_TARGET - COGS_TARGET
    OP_TARGET = GP_TARGET - OPEX_TARGET

    df_with_targets = df_pl \
        .withColumn("target_revenue", lit(REVENUE_TARGET).cast(DecimalType(18,2))) \
        .withColumn("target_cogs", lit(COGS_TARGET).cast(DecimalType(18,2))) \
        .withColumn("target_gross_profit", lit(GP_TARGET).cast(DecimalType(18,2))) \
        .withColumn("target_opex", lit(OPEX_TARGET).cast(DecimalType(18,2))) \
        .withColumn("target_operating_profit", lit(OP_TARGET).cast(DecimalType(18,2)))

    # Step 6: Calculate variances
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

    # Step 7: Calculate scores
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

    df_with_scores = df_with_scores \
        .withColumn("pl_health_score",
            ((col("revenue_score") * PL_WEIGHTS["revenue"]) +
             (col("cogs_score") * PL_WEIGHTS["cogs"]) +
             (col("gross_profit_score") * PL_WEIGHTS["gross_profit"]) +
             (col("opex_score") * PL_WEIGHTS["opex"]) +
             (col("operating_profit_score") * PL_WEIGHTS["operating_profit"])
            ).cast(FloatType()))

    # Step 8: Final output
    result = df_with_scores.select(
        col("invoice_id").cast(StringType()).alias("invoice_id"),
        col("invoice_date_key").cast(IntegerType()).alias("invoice_date_key"),
        lit(None).cast(LongType()).alias("inventory_key"),
        col("location_id").cast(LongType()).alias("location_key"),
        col("account_id").cast(StringType()).alias("account_id"),

        col("total_revenue").cast(DecimalType(18,2)).alias("total_revenue"),
        col("cost_of_sales").cast(DecimalType(18,2)).alias("cost_of_sales"),
        col("gross_profit").cast(DecimalType(18,2)).alias("gross_profit"),
        col("operating_expenses").cast(DecimalType(18,2)).alias("operating_expenses"),
        col("operating_profit").cast(DecimalType(18,2)).alias("operating_profit"),

        col("gross_profit_margin").cast(FloatType()).alias("gross_profit_margin"),
        col("operating_profit_margin").cast(FloatType()).alias("operating_profit_margin"),

        col("target_revenue").cast(DecimalType(18,2)).alias("target_revenue"),
        col("target_cogs").cast(DecimalType(18,2)).alias("target_cogs"),
        col("target_gross_profit").cast(DecimalType(18,2)).alias("target_gross_profit"),
        col("target_opex").cast(DecimalType(18,2)).alias("target_opex"),
        col("target_operating_profit").cast(DecimalType(18,2)).alias("target_operating_profit"),

        col("revenue_variance").cast(DecimalType(18,2)).alias("revenue_variance"),
        col("revenue_variance_pct").cast(FloatType()).alias("revenue_variance_pct"),
        col("cogs_variance").cast(DecimalType(18,2)).alias("cogs_variance"),
        col("opex_variance").cast(DecimalType(18,2)).alias("opex_variance"),

        col("revenue_score").cast(FloatType()).alias("revenue_score"),
        col("cogs_score").cast(FloatType()).alias("cogs_score"),
        col("gross_profit_score").cast(FloatType()).alias("gross_profit_score"),
        col("opex_score").cast(FloatType()).alias("opex_score"),
        col("operating_profit_score").cast(FloatType()).alias("operating_profit_score"),
        col("pl_health_score").cast(FloatType()).alias("pl_health_score"),

        current_timestamp().alias("dw_created_at"),
        current_timestamp().alias("dw_updated_at")
    )

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start agg_pl")
try:
    df = create_agg_pl()
    src_cnt = 0
    tgt_cnt = df.count()

    target_table = AGG_TABLES["pl"]

    # Write to Delta table
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

    # Print summary
    # end_job_instance(batch_id, job_id, "SUCCESS", msg="Created agg_pl", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print(f"✓ Saved to catalog table: {TARGET_TABLE}")
    print(f"\n=== P&L Summary ===")
    df.selectExpr(
        "SUM(total_revenue) as total_revenue",
        "SUM(cost_of_sales) as total_cogs",
        "SUM(gross_profit) as total_gross_profit",
        "SUM(operating_expenses) as total_opex",
        "SUM(operating_profit) as total_operating_profit"
    ).show(truncate=False)

    print("\n=== Performance Scores ===")
    df.selectExpr(
        "ROUND(AVG(revenue_score), 2) as avg_revenue_score",
        "ROUND(AVG(gross_profit_score), 2) as avg_gp_score",
        "ROUND(AVG(operating_profit_score), 2) as avg_op_score",
        "ROUND(AVG(pl_health_score), 2) as avg_health_score"
    ).show()

except Exception as e:
    # end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed agg_pl. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select * from `WS-ETL-BNJ`.lh_bnj_gold.gold.agg_pl
# MAGIC order by invoice_date_key desc
# MAGIC limit 100

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
