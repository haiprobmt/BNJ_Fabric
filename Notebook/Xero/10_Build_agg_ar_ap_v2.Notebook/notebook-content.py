# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Build agg_ar_ap - AR/AP Aging & Payment Analytics (v2)
# # MAGIC
# **Updated to handle actual Xero data structure from Fabric**
# # MAGIC
# This notebook builds the AR/AP aging analysis aggregate table with the following metrics:
# - **AR Aging Buckets**: 0-30, 31-60, 61-90, 91-180, 180+ days
# - **AP Aging Buckets**: 0-30, 31-60, 61-90, 91-180, 180+ days
# - **Payment Timing**: Early, On-time, Late payment counts
# - **Health Scores**: AR score, AP score, combined AR/AP health score

# MARKDOWN ********************

# ## 1. Parameters

# CELL ********************

dbutils.widgets.text("bronze_table_invoices", "invoices", "Bronze Table: Invoices")
dbutils.widgets.text("bronze_table_payments", "payments", "Bronze Table: Payments")
dbutils.widgets.text("gold_table", "agg_ar_ap", "Gold Table Name")
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
bronze_invoices_table = dbutils.widgets.get("bronze_table_invoices")
bronze_payments_table = dbutils.widgets.get("bronze_table_payments")
gold_table = dbutils.widgets.get("gold_table")
as_of_date_str = dbutils.widgets.get("as_of_date")

# Set as_of_date to today if not provided
if as_of_date_str:
    as_of_date = datetime.strptime(as_of_date_str, '%Y-%m-%d').date()
else:
    as_of_date = date.today()

print(f"Building {gold_table} as of date: {as_of_date}")
print(f"Source tables: {bronze_invoices_table}, {bronze_payments_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Load and Inspect Source Data

# CELL ********************

# Load bronze tables
invoices_df = spark.table(bronze_invoices_table)
payments_df = spark.table(bronze_payments_table)

print(f"Invoices schema:")
invoices_df.printSchema()
print(f"\nInvoices count: {invoices_df.count():,}")

print(f"\nPayments schema:")
payments_df.printSchema()
print(f"Payments count: {payments_df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Detect and Flatten Invoice Records
# # MAGIC
# Handle two possible structures:
# - Case 1: records is an ARRAY of structs (needs explode)
# - Case 2: records is a single STRUCT (direct access)

# CELL ********************

# Check if records is an array or struct
records_type = [f for f in invoices_df.schema.fields if f.name == 'records'][0].dataType
is_array = 'Array' in str(records_type)

print(f"Records field type: {records_type}")
print(f"Is array: {is_array}")

if is_array:
    print("Using EXPLODE strategy...")
    # Records is an array - need to explode
    invoices_exploded = invoices_df.select(
        F.col("tenant_id"),
        F.col("extraction_id"),
        F.col("extracted_at"),
        F.posexplode("records").alias("pos", "record")
    )

    # Now select from exploded records
    invoices_flat = invoices_exploded.select(
        "tenant_id",
        "extraction_id",
        "extracted_at",
        F.col("record.invoice_id").alias("invoice_id"),
        F.col("record.invoice_number").alias("invoice_number"),
        F.col("record.type").alias("invoice_type"),
        F.col("record.status").alias("status"),
        F.to_date(F.col("record.date")).alias("invoice_date"),
        F.to_date(F.col("record.due_date")).alias("due_date"),
        F.coalesce(F.col("record.total").cast("double"), F.lit(0.0)).alias("total_amount"),
        F.coalesce(F.col("record.amount_due").cast("double"), F.lit(0.0)).alias("amount_due"),
        F.coalesce(F.col("record.amount_paid").cast("double"), F.lit(0.0)).alias("amount_paid"),
        F.col("record.contact.contact_id").alias("contact_id"),
        F.col("record.contact.name").alias("contact_name")
    )
else:
    print("Using DIRECT ACCESS strategy...")
    # Records is a struct - direct access
    invoices_flat = invoices_df.select(
        "tenant_id",
        "extraction_id",
        "extracted_at",
        F.col("records.invoice_id").alias("invoice_id"),
        F.col("records.invoice_number").alias("invoice_number"),
        F.col("records.type").alias("invoice_type"),
        F.col("records.status").alias("status"),
        F.to_date(F.col("records.date")).alias("invoice_date"),
        F.to_date(F.col("records.due_date")).alias("due_date"),
        F.coalesce(F.col("records.total").cast("double"), F.lit(0.0)).alias("total_amount"),
        F.coalesce(F.col("records.amount_due").cast("double"), F.lit(0.0)).alias("amount_due"),
        F.coalesce(F.col("records.amount_paid").cast("double"), F.lit(0.0)).alias("amount_paid"),
        F.col("records.contact.contact_id").alias("contact_id"),
        F.col("records.contact.name").alias("contact_name")
    )

# Filter to only AUTHORISED and PAID invoices
invoices_flat = invoices_flat.filter(
    F.col("status").isin("AUTHORISED", "PAID")
)

print(f"\nFlattened invoices: {invoices_flat.count():,} records")
display(invoices_flat.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Flatten Payment Data

# CELL ********************

# Check payments structure
payments_records_type = [f for f in payments_df.schema.fields if f.name == 'records'][0].dataType
payments_is_array = 'Array' in str(payments_records_type)

print(f"Payments records type: {payments_records_type}")
print(f"Is array: {payments_is_array}")

if payments_is_array:
    payments_exploded = payments_df.select(
        F.col("tenant_id"),
        F.posexplode("records").alias("pos", "payment")
    )

    payments_flat = payments_exploded.select(
        "tenant_id",
        F.col("payment.payment_id").alias("payment_id"),
        F.col("payment.invoice.invoice_id").alias("invoice_id"),
        F.to_date(F.col("payment.date")).alias("payment_date"),
        F.coalesce(F.col("payment.amount").cast("double"), F.lit(0.0)).alias("payment_amount")
    )
else:
    payments_flat = payments_df.select(
        "tenant_id",
        F.col("records.payment_id").alias("payment_id"),
        F.col("records.invoice.invoice_id").alias("invoice_id"),
        F.to_date(F.col("records.date")).alias("payment_date"),
        F.coalesce(F.col("records.amount").cast("double"), F.lit(0.0)).alias("payment_amount")
    )

payments_flat = payments_flat.filter(F.col("invoice_id").isNotNull())

print(f"\nFlattened payments: {payments_flat.count():,} records")
display(payments_flat.limit(5))

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

# Classify into aging buckets
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

# ## 7. Calculate Payment Timing

# CELL ********************

# Join invoices with payments
invoices_with_payments = invoices_flat.join(
    payments_flat,
    on=["tenant_id", "invoice_id"],
    how="left"
).filter(
    F.col("payment_date").isNotNull()
)

# Calculate payment timing
invoices_with_payments = invoices_with_payments.withColumn(
    "days_diff",
    F.datediff(F.col("payment_date"), F.col("due_date"))
).withColumn(
    "payment_timing",
    F.when(F.col("days_diff") < 0, "early")
    .when(F.col("days_diff") == 0, "on_time")
    .otherwise("late")
)

# AP payments are ACCPAY invoices (bills we pay)
ap_payments = invoices_with_payments.filter(F.col("invoice_type") == "ACCPAY")

print(f"Paid AP invoices: {ap_payments.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Aggregate AR Metrics

# CELL ********************

ar_aging = ar_invoices.filter(F.col("amount_due") > 0).groupBy("tenant_id").agg(
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

    # Totals
    F.sum(F.col("amount_due")).alias("total_ar_amount"),
    F.round(F.avg(F.col("days_outstanding")), 2).alias("ar_days_outstanding")
)

print(f"AR aggregates: {ar_aging.count():,} tenants")
display(ar_aging)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Aggregate AP Metrics

# CELL ********************

ap_aging = ap_invoices.filter(F.col("amount_due") > 0).groupBy("tenant_id").agg(
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

    # Totals
    F.sum(F.col("amount_due")).alias("total_ap_amount"),
    F.round(F.avg(F.col("days_outstanding")), 2).alias("ap_days_outstanding")
)

print(f"AP aggregates: {ap_aging.count():,} tenants")
display(ap_aging)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Aggregate Payment Timing

# CELL ********************

ap_payment_timing = ap_payments.groupBy("tenant_id").agg(
    F.count(F.when(F.col("payment_timing") == "early", 1)).alias("ap_paid_early_count"),
    F.count(F.when(F.col("payment_timing") == "on_time", 1)).alias("ap_paid_on_time_count"),
    F.count(F.when(F.col("payment_timing") == "late", 1)).alias("ap_paid_late_count")
)

print(f"AP payment timing: {ap_payment_timing.count():,} tenants")
display(ap_payment_timing)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Calculate Health Scores

# CELL ********************

# Join all metrics
agg_base = ar_aging.join(ap_aging, on="tenant_id", how="outer").join(
    ap_payment_timing, on="tenant_id", how="left"
).fillna(0)

# AR Aging Score (0-100, penalize older buckets)
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

# AP Aging Score
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

# Combined AR/AP Health Score
agg_base = agg_base.withColumn(
    "ar_ap_health_score",
    F.round((F.col("ar_aging_score") + F.col("ap_aging_score")) / 2, 2)
)

display(agg_base)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Add Final Dimensions and Metadata

# CELL ********************

agg_ar_ap_final = agg_base.withColumn(
    "report_date", F.lit(as_of_date)
).withColumn(
    "date_key", F.date_format(F.lit(as_of_date), "yyyyMMdd").cast("int")
).withColumn(
    "invoice_date_key", F.date_format(F.lit(as_of_date), "yyyyMMdd").cast("int")
).withColumn(
    "invoice_id", F.lit(None).cast("string")
).withColumn(
    "corporate_id", F.col("tenant_id")
).withColumn(
    "location_key", F.lit(None).cast("int")
).withColumn(
    "inventory_key", F.lit(None).cast("int")
).withColumn(
    "dw_created_at", F.current_timestamp()
).withColumn(
    "dw_updated_at", F.current_timestamp()
).select(
    "corporate_id",
    "date_key",
    "invoice_date_key",
    "invoice_id",
    "location_key",
    "inventory_key",
    "report_date",
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
    "ap_paid_early_count",
    "ap_paid_on_time_count",
    "ap_paid_late_count",
    "ar_ap_health_score",
    "dw_created_at",
    "dw_updated_at"
)

print(f"Final aggregate: {agg_ar_ap_final.count():,} rows")
display(agg_ar_ap_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Write to Gold Table

# CELL ********************

# Write as table
agg_ar_ap_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table)

print(f"Successfully created table: {gold_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 14. Summary

# CELL ********************

print("="*70)
print(f"AGG_AR_AP BUILD SUMMARY")
print("="*70)
print(f"As of Date: {as_of_date}")
print(f"Records: {agg_ar_ap_final.count():,}")
print(f"\nAR Metrics:")
print(f"  Total AR: ${agg_ar_ap_final.select(F.sum('total_ar_amount')).collect()[0][0]:,.2f}")
print(f"  AR Score: {agg_ar_ap_final.select(F.avg('ar_aging_score')).collect()[0][0]:.2f}")
print(f"\nAP Metrics:")
print(f"  Total AP: ${agg_ar_ap_final.select(F.sum('total_ap_amount')).collect()[0][0]:,.2f}")
print(f"  AP Score: {agg_ar_ap_final.select(F.avg('ap_aging_score')).collect()[0][0]:.2f}")
print(f"\nHealth Score: {agg_ar_ap_final.select(F.avg('ar_ap_health_score')).collect()[0][0]:.2f}")
print("="*70)

dbutils.notebook.exit("SUCCESS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
