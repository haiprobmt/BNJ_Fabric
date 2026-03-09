# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c9d7507e-938a-4c6d-a042-d8743e386ab5",
# META       "default_lakehouse_name": "lh_bnj_bronze",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
# META         },
# META         {
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Build agg_ar_ap - AR/AP Aging & Payment Analytics
# # MAGIC
# This notebook builds the AR/AP aging analysis aggregate table with the following metrics:
# - **AR Aging Buckets**: 0-30, 31-60, 61-90, 91-180, 180+ days
# - **AP Aging Buckets**: 0-30, 31-60, 61-90, 91-180, 180+ days
# - **Payment Timing**: Early, On-time, Late payment counts
# - **Health Scores**: AR score, AP score, combined AR/AP health score
# # MAGIC
# ## Source Tables:
# - `invoices` - AR (ACCREC) and AP (ACCPAY) invoices
# - `payments` - Payment transactions
# - `credit_notes` - Credit note allocations
# # MAGIC
# ## Target Table:
# - `gold_agg_ar_ap` - Aggregated AR/AP metrics by date

# MARKDOWN ********************

# ## 1. Parameters

# CELL ********************

dbutils.widgets.text("bronze_path", "/lakehouse/default/Files/bronze/xero", "Bronze Path")
dbutils.widgets.text("gold_path", "/lakehouse/default/Files/gold", "Gold Path")
dbutils.widgets.text("as_of_date", "", "As Of Date (YYYY-MM-DD, blank=today)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Setup and Imports

# CELL ********************

from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
from datetime import datetime, date

# Get parameters
bronze_path = "abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Tables/xero/"
gold_path = "abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/102823e0-12f1-4ca5-b61b-a2df5d75beb2/Tables/gold/"
as_of_date_str = None

# Set as_of_date to today if not provided
if as_of_date_str:
    as_of_date = datetime.strptime(as_of_date_str, '%Y-%m-%d').date()
else:
    as_of_date = date.today()

print(f"Building agg_ar_ap as of date: {as_of_date}")
print(f"Bronze path: {bronze_path}")
print(f"Gold path: {gold_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Load Source Data

# CELL ********************

# Load bronze tables
try:
    invoices_df = spark.read.format("delta").load(f"{bronze_path}/invoices")
    payments_df = spark.read.format("delta").load(f"{bronze_path}/payments")
    credit_notes_df = spark.read.format("delta").load(f"{bronze_path}/credit_notes")

    print(f"Loaded {invoices_df.count():,} invoices")
    print(f"Loaded {payments_df.count():,} payments")
    print(f"Loaded {credit_notes_df.count():,} credit notes")
except Exception as e:
    print(f"Error loading source tables: {e}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Flatten and Prepare Invoice Data

# CELL ********************

display(invoices_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Flatten invoices with exploded records
invoices_flat = invoices_df.select(
    "tenant_id",
    "extraction_id",
    "extracted_at",
    F.col("`records.invoice_id`").alias("invoice_id"),
    F.col("`records.invoice_number`").alias("invoice_number"),
    F.col("`records.type`").alias("invoice_type"),
    F.col("`records.status`").alias("status"),
    F.to_date(F.col("`records.date`")).alias("invoice_date"),
    F.to_date(F.col("`records.due_date`")).alias("due_date"),
    F.coalesce(F.col("`records.total`"), F.lit(0.0)).alias("total_amount"),
    F.coalesce(F.col("`records.amount_due`"), F.lit(0.0)).alias("amount_due"),
    F.coalesce(F.col("`records.amount_paid`"), F.lit(0.0)).alias("amount_paid"),
    F.col("`records.contact.contact_id`").alias("contact_id"),
    F.col("`records.contact.name`").alias("contact_name")
)

# Filter to only AUTHORISED and PAID invoices (exclude DRAFT, VOIDED, etc.)
invoices_flat = invoices_flat.filter(
    F.col("status").isin("AUTHORISED", "PAID")
)

print(f"Flattened to {invoices_flat.count():,} invoice records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Flatten Payment Data

# CELL ********************

# Flatten payments
payments_flat = payments_df.select(
    F.col("`records.payment_id`").alias("payment_id"),
    F.col("`records.invoice.invoice_id`").alias("invoice_id"),
    F.to_date(F.col("`records.date`")).alias("payment_date"),
    F.coalesce(F.col("`records.amount`"), F.lit(0.0)).alias("payment_amount")
).filter(
    F.col("invoice_id").isNotNull()
)

print(f"Flattened to {payments_flat.count():,} payment records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Calculate Aging for Outstanding Invoices

# CELL ********************

# Calculate days outstanding as of the as_of_date
invoices_with_aging = invoices_flat.withColumn(
    "days_outstanding",
    F.datediff(F.lit(as_of_date), F.col("due_date"))
).withColumn(
    "days_from_invoice",
    F.datediff(F.lit(as_of_date), F.col("invoice_date"))
)

# Classify into aging buckets (only for outstanding amounts)
invoices_with_aging = invoices_with_aging.withColumn(
    "aging_bucket",
    F.when(F.col("amount_due") <= 0, "PAID")
    .when(F.col("days_outstanding") <= 30, "0-30")
    .when(F.col("days_outstanding") <= 60, "31-60")
    .when(F.col("days_outstanding") <= 90, "61-90")
    .when(F.col("days_outstanding") <= 180, "91-180")
    .otherwise("180+")
)

# Separate AR and AP
ar_invoices = invoices_with_aging.filter(F.col("invoice_type") == "ACCREC")
ap_invoices = invoices_with_aging.filter(F.col("invoice_type") == "ACCPAY")

print(f"AR invoices: {ar_invoices.count():,}")
print(f"AP invoices: {ap_invoices.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Calculate Payment Timing (Early/On-Time/Late)

# CELL ********************

# Join invoices with payments to get payment timing
invoices_with_payments = invoices_flat.join(
    payments_flat,
    on="invoice_id",
    how="left"
).filter(
    F.col("payment_date").isNotNull()  # Only fully paid invoices
)

# Calculate if payment was early, on-time, or late
invoices_with_payments = invoices_with_payments.withColumn(
    "days_diff",
    F.datediff(F.col("payment_date"), F.col("due_date"))
).withColumn(
    "payment_timing",
    F.when(F.col("days_diff") < 0, "early")
    .when(F.col("days_diff") == 0, "on_time")
    .otherwise("late")
)

# Separate AR and AP payment timing
ar_payments = invoices_with_payments.filter(F.col("invoice_type") == "ACCPAY")  # AP payments (we pay)
ap_payments = invoices_with_payments.filter(F.col("invoice_type") == "ACCREC")  # AR payments (we receive)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Aggregate AR Metrics

# CELL ********************

# AR aging buckets (outstanding receivables)
ar_aging = ar_invoices.filter(F.col("amount_due") > 0).groupBy("invoice_id", "invoice_date").agg(
    # 0-30 days
    F.sum(F.when(F.col("aging_bucket") == "0-30", F.col("amount_due")).otherwise(0)).alias("ar_0_30_days_amount"),
    F.count(F.when(F.col("aging_bucket") == "0-30", 1)).alias("ar_0_30_days_count"),

    # 31-60 days
    F.sum(F.when(F.col("aging_bucket") == "31-60", F.col("amount_due")).otherwise(0)).alias("ar_31_60_days_amount"),
    F.count(F.when(F.col("aging_bucket") == "31-60", 1)).alias("ar_31_60_days_count"),

    # 61-90 days
    F.sum(F.when(F.col("aging_bucket") == "61-90", F.col("amount_due")).otherwise(0)).alias("ar_61_90_days_amount"),
    F.count(F.when(F.col("aging_bucket") == "61-90", 1)).alias("ar_61_90_days_count"),

    # 91-180 days
    F.sum(F.when(F.col("aging_bucket") == "91-180", F.col("amount_due")).otherwise(0)).alias("ar_91_180_days_amount"),
    F.count(F.when(F.col("aging_bucket") == "91-180", 1)).alias("ar_91_180_days_count"),

    # Over 180 days
    F.sum(F.when(F.col("aging_bucket") == "180+", F.col("amount_due")).otherwise(0)).alias("ar_over_180_days_amount"),
    F.count(F.when(F.col("aging_bucket") == "180+", 1)).alias("ar_over_180_days_count"),

    # Total AR and weighted average days outstanding
    F.sum(F.col("amount_due")).alias("total_ar_amount"),
    F.avg(F.col("days_outstanding")).alias("ar_days_outstanding")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Aggregate AP Metrics

# CELL ********************

# AP aging buckets (outstanding payables)
ap_aging = ap_invoices.filter(F.col("amount_due") > 0).groupBy("invoice_id", "due_date").agg(
    # 0-30 days
    F.sum(F.when(F.col("aging_bucket") == "0-30", F.col("amount_due")).otherwise(0)).alias("ap_0_30_days_amount"),
    F.count(F.when(F.col("aging_bucket") == "0-30", 1)).alias("ap_0_30_days_count"),

    # 31-60 days
    F.sum(F.when(F.col("aging_bucket") == "31-60", F.col("amount_due")).otherwise(0)).alias("ap_31_60_days_amount"),
    F.count(F.when(F.col("aging_bucket") == "31-60", 1)).alias("ap_31_60_days_count"),

    # 61-90 days
    F.sum(F.when(F.col("aging_bucket") == "61-90", F.col("amount_due")).otherwise(0)).alias("ap_61_90_days_amount"),
    F.count(F.when(F.col("aging_bucket") == "61-90", 1)).alias("ap_61_90_days_count"),

    # 91-180 days
    F.sum(F.when(F.col("aging_bucket") == "91-180", F.col("amount_due")).otherwise(0)).alias("ap_91_180_days_amount"),
    F.count(F.when(F.col("aging_bucket") == "91-180", 1)).alias("ap_91_180_days_count"),

    # Over 180 days
    F.sum(F.when(F.col("aging_bucket") == "180+", F.col("amount_due")).otherwise(0)).alias("ap_over_180_days_amount"),
    F.count(F.when(F.col("aging_bucket") == "180+", 1)).alias("ap_over_180_days_count"),

    # Total AP and weighted average days outstanding
    F.sum(F.col("amount_due")).alias("total_ap_amount"),
    F.avg(F.col("days_outstanding")).alias("ap_days_outstanding")
)
ap_aging = ap_aging.withColumn('invoice_date', F.col('due_date'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Aggregate Payment Timing Metrics

# CELL ********************

# AP payment timing (how we pay our bills)
ap_payment_timing = ar_payments.groupBy("invoice_id").agg(
    F.count(F.when(F.col("payment_timing") == "early", 1)).alias("ap_paid_early_count"),
    F.count(F.when(F.col("payment_timing") == "on_time", 1)).alias("ap_paid_on_time_count"),
    F.count(F.when(F.col("payment_timing") == "late", 1)).alias("ap_paid_late_count")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Calculate Health Scores

# CELL ********************

# Join AR and AP metrics
agg_base = ar_aging.join(ap_aging, on=["invoice_id", "invoice_date"], how="outer").fillna(0)
# Calculate AR Aging Score (0-100, higher is better)
# Logic: Penalize older buckets more heavily
agg_base = agg_base.withColumn(
    "ar_aging_score",
    F.round(
        100 - (
            (F.col("ar_31_60_days_amount") / F.greatest(F.col("total_ar_amount"), F.lit(1))) * 10 +
            (F.col("ar_61_90_days_amount") / F.greatest(F.col("total_ar_amount"), F.lit(1))) * 25 +
            (F.col("ar_91_180_days_amount") / F.greatest(F.col("total_ar_amount"), F.lit(1))) * 40 +
            (F.col("ar_over_180_days_amount") / F.greatest(F.col("total_ar_amount"), F.lit(1))) * 60
        ), 2
    )
)

# Calculate AP Aging Score (0-100, higher is better)
agg_base = agg_base.withColumn(
    "ap_aging_score",
    F.round(
        100 - (
            (F.col("ap_31_60_days_amount") / F.greatest(F.col("total_ap_amount"), F.lit(1))) * 10 +
            (F.col("ap_61_90_days_amount") / F.greatest(F.col("total_ap_amount"), F.lit(1))) * 25 +
            (F.col("ap_91_180_days_amount") / F.greatest(F.col("total_ap_amount"), F.lit(1))) * 40 +
            (F.col("ap_over_180_days_amount") / F.greatest(F.col("total_ap_amount"), F.lit(1))) * 60
        ), 2
    )
)

# Calculate overall AR/AP Health Score (average of AR and AP scores)
agg_base = agg_base.withColumn(
    "ar_ap_health_score",
    F.round((F.col("ar_aging_score") + F.col("ap_aging_score")) / 2, 2)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Add Dimension Keys and Metadata

# CELL ********************

# Add date dimension keys and metadata
agg_ar_ap = agg_base.withColumn(
    "report_date", F.lit(as_of_date)
).withColumn(
    "date_key", F.date_format(F.lit(as_of_date), "yyyyMMdd").cast("int")
).withColumn(
    "location_key", F.lit(None).cast("int")  # Placeholder for location dimension
).withColumn(
    "inventory_key", F.lit(None).cast("int")  # Placeholder for inventory dimension
).withColumn(
    "dw_created_at", F.current_timestamp()
).withColumn(
    "dw_updated_at", F.current_timestamp()
)

# Select final columns in order
agg_ar_ap_final = agg_ar_ap.select(
    # Dimensions
    # "corporate_id",
    # "tenant_id",
    "date_key",
    "invoice_date",
    "invoice_id",
    "location_key",
    "inventory_key",
    "report_date",

    # AR Metrics
    "ar_0_30_days_amount",
    "ar_0_30_days_count",
    "ar_31_60_days_amount",
    "ar_31_60_days_count",
    "ar_61_90_days_amount",
    "ar_61_90_days_count",
    "ar_91_180_days_amount",
    "ar_91_180_days_count",
    "ar_over_180_days_amount",
    "ar_over_180_days_count",
    "ar_days_outstanding",
    "ar_aging_score",
    "total_ar_amount",

    # AP Metrics
    "ap_0_30_days_amount",
    "ap_0_30_days_count",
    "ap_31_60_days_amount",
    "ap_31_60_days_count",
    "ap_61_90_days_amount",
    "ap_61_90_days_count",
    "ap_91_180_days_amount",
    "ap_91_180_days_count",
    "ap_over_180_days_amount",
    "ap_over_180_days_count",
    "ap_days_outstanding",
    "ap_aging_score",
    "total_ap_amount",

    # # Payment Timing
    # "ap_paid_early_count",
    # "ap_paid_on_time_count",
    # "ap_paid_late_count",

    # Health Score
    "ar_ap_health_score",

    # Metadata
    "dw_created_at",
    "dw_updated_at"
)

print(f"Final aggregate has {agg_ar_ap_final.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Preview Results

# CELL ********************

display(agg_ar_ap_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 14. Write to Gold Layer

# CELL ********************

# Write as Delta table with overwrite mode
output_path = f"{gold_path}/agg_ar_ap"

agg_ar_ap_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(output_path)

print(f"Successfully wrote agg_ar_ap to {output_path}")

# Create or replace table in metastore
agg_ar_ap_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_agg_ar_ap")

print("Successfully created/updated table: gold_agg_ar_ap")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 15. Summary Statistics

# CELL ********************

print("="*70)
print("AGG_AR_AP BUILD SUMMARY")
print("="*70)
print(f"As of Date: {as_of_date}")
print(f"Records Created: {agg_ar_ap_final.count():,}")
print(f"\nAR Metrics:")
print(f"  Total AR: ${agg_ar_ap_final.select(F.sum('total_ar_amount')).collect()[0][0]:,.2f}")
print(f"  AR Aging Score: {agg_ar_ap_final.select(F.avg('ar_aging_score')).collect()[0][0]:.2f}")
print(f"\nAP Metrics:")
print(f"  Total AP: ${agg_ar_ap_final.select(F.sum('total_ap_amount')).collect()[0][0]:,.2f}")
print(f"  AP Aging Score: {agg_ar_ap_final.select(F.avg('ap_aging_score')).collect()[0][0]:.2f}")
print(f"\nOverall Health Score: {agg_ar_ap_final.select(F.avg('ar_ap_health_score')).collect()[0][0]:.2f}")
print("="*70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dbutils.notebook.exit("SUCCESS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
