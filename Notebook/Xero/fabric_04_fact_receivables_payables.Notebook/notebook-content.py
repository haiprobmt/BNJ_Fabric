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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Fact ETL - Receivables & Payables Snapshots
# 
# **Purpose:** Daily snapshot of outstanding AR and AP
# 
# **Runtime:** ~5 minutes
# 
# **Schedule:** Daily
# 
# **Refresh Strategy:** Full snapshot (delete today, insert fresh)
# 
# ---

# MARKDOWN ********************

# ## Configuration

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import time

# Configuration
bronze_schema = "lh_bnj_bronze.xero"
gold_schema = "lh_bnj_gold.gold"

start_time = time.time()
snapshot_date = datetime.now().date()
snapshot_date_key = int(snapshot_date.strftime('%Y%m%d'))

print(f"Creating snapshots for {snapshot_date}")
print(f"Snapshot date key: {snapshot_date_key}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 1: Receivables Snapshot

# MARKDOWN ********************

# ### Delete today's snapshot (if re-running)

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM gold.fact_receivables
# MAGIC WHERE snapshot_date_key = {snapshot_date_key}

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Extract outstanding receivables

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold.fact_receivables;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_gold.gold.fact_receivables (
# MAGIC     receivable_key BIGINT,
# MAGIC     invoice_date_key INT,
# MAGIC     due_date_key INT,
# MAGIC     snapshot_date_key INT,
# MAGIC     contact_key BIGINT,
# MAGIC     invoice_id STRING,
# MAGIC     invoice_number STRING,
# MAGIC     total_amount DECIMAL(18,2),
# MAGIC     amount_paid DECIMAL(18,2),
# MAGIC     amount_due DECIMAL(18,2),
# MAGIC     days_outstanding INT,
# MAGIC     aging_bucket STRING,
# MAGIC     invoice_status STRING,
# MAGIC     currency_code STRING,
# MAGIC     tenant_id STRING,
# MAGIC     created_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (snapshot_date_key);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read invoices and contact dimension
invoices = spark.table(f"{bronze_schema}.invoices")
dim_contact = spark.table(f"{gold_schema}.dim_contact").where(col('is_current') == True)

# Extract receivables
receivables_df = invoices \
    .where(col("`records.type`") == 'ACCREC') \
    .where(col("`records.status`") != 'VOIDED') \
    .where(col("`records.amount_due`").cast('decimal(18,2)') > 0) \
    .select(
        date_format(to_date(col("`records.date`")), 'yyyyMMdd').cast('int').alias('invoice_date_key'),
        date_format(to_date(col("`records.due_date`")), 'yyyyMMdd').cast('int').alias('due_date_key'),
        lit(snapshot_date_key).alias('snapshot_date_key'),
        col("`records.contact.contact_id`").alias('contact_id'),
        col("`records.invoice_id`").alias('invoice_id'),
        col("`records.invoice_number`").alias('invoice_number'),
        col("`records.total`").cast('decimal(18,2)').alias('total_amount'),
        col("`records.amount_paid`").cast('decimal(18,2)').alias('amount_paid'),
        col("`records.amount_due`").cast('decimal(18,2)').alias('amount_due'),
        col("`records.status`").alias('invoice_status'),
        coalesce(col("`records.currency_code`"), lit('SGD')).alias('currency_code'),
        col('tenant_id')
    )
# Calculate days outstanding and aging bucket
receivables_df = receivables_df.withColumn(
        "days_outstanding",
        col("snapshot_date_key") - col("invoice_date_key")
    ) \
    .withColumn(
        'aging_bucket',
        when(col('days_outstanding') <= 30, 'Current (0-30 days)')
        .when(col('days_outstanding') <= 60, '1 Month (31-60 days)')
        .when(col('days_outstanding') <= 90, '2 Months (61-90 days)')
        .otherwise('3+ Months (90+ days)')
    )

# Join with contacts
receivables_with_contact = receivables_df \
    .join(
        dim_contact.select('contact_id', 'contact_key'),
        'contact_id',
        'left'
    )

# Get next receivable_key
max_key = spark.sql(f"SELECT COALESCE(MAX(receivable_key), 0) as max_key FROM {gold_schema}.fact_receivables").collect()[0]['max_key']

# Prepare final dataset
receivables_final = receivables_with_contact \
    .withColumn('receivable_key', monotonically_increasing_id() + max_key + 1) \
    .withColumn('created_date', current_timestamp()) \
    .select(
        'receivable_key', 'invoice_date_key', 'due_date_key', 'snapshot_date_key',
        'contact_key', 'invoice_id', 'invoice_number', 'total_amount', 'amount_paid',
        'amount_due', 'days_outstanding', 'aging_bucket', 'invoice_status',
        'currency_code', 'tenant_id', 'created_date'
    )

# Write to Delta
receivables_final.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable(f"{gold_schema}.fact_receivables")
# display(receivables_final)
receivables_count = receivables_final.count()
total_ar = receivables_final.agg(sum('amount_due')).collect()[0][0]

print(f"✓ Receivables snapshot: {receivables_count} invoices")
print(f"  Total outstanding: ${total_ar:,.2f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 2: Payables Snapshot

# MARKDOWN ********************

# ### Delete today's snapshot

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM gold.fact_payables
# MAGIC WHERE snapshot_date_key = {snapshot_date_key}

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Extract outstanding payables

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold.fact_payables;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_gold.gold.fact_payables (
# MAGIC     payable_key BIGINT,
# MAGIC     invoice_date_key INT,
# MAGIC     due_date_key INT,
# MAGIC     snapshot_date_key INT,
# MAGIC     contact_key BIGINT,
# MAGIC     bill_id STRING,
# MAGIC     bill_number STRING,
# MAGIC     total_amount DECIMAL(18,2),
# MAGIC     amount_paid DECIMAL(18,2),
# MAGIC     amount_due DECIMAL(18,2),
# MAGIC     days_overdue INT,
# MAGIC     aging_bucket STRING,
# MAGIC     bill_status STRING,
# MAGIC     currency_code STRING,
# MAGIC     tenant_id STRING,
# MAGIC     created_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (snapshot_date_key);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract payables
payables_df = invoices \
    .where(col("`records.type`") == 'ACCPAY') \
    .where(col("`records.status`") != 'VOIDED') \
    .where(col("`records.amount_due`").cast('decimal(18,2)') > 0) \
    .select(
        date_format(to_date(col("`records.date`")), 'yyyyMMdd').cast('int').alias('invoice_date_key'),
        date_format(to_date(col("`records.due_date`")), 'yyyyMMdd').cast('int').alias('due_date_key'),
        lit(snapshot_date_key).alias('snapshot_date_key'),
        col("`records.contact.contact_id`").alias('contact_id'),
        col("`records.invoice_id`").alias('bill_id'),
        col("`records.invoice_number`").alias('bill_number'),
        col("`records.total`").cast('decimal(18,2)').alias('total_amount'),
        col("`records.amount_paid`").cast('decimal(18,2)').alias('amount_paid'),
        col("`records.amount_due`").cast('decimal(18,2)').alias('amount_due'),
        col("`records.status`").alias('bill_status'),
        coalesce(col("`records.currency_code`"), lit('SGD')).alias('currency_code'),
        col('tenant_id')
    )

# Calculate days overdue and aging bucket (based on due date)
payables_df = payables_df.withColumn(
        "days_overdue",
        col("snapshot_date_key") - col("due_date_key")
    ) \
    .withColumn(
        'aging_bucket',
        when(col('days_overdue') < 0, 'Not Yet Due')
        .when(col('days_overdue') <= 30, 'Current (0-30 days)')
        .when(col('days_overdue') <= 60, '1 Month (31-60 days)')
        .when(col('days_overdue') <= 90, '2 Months (61-90 days)')
        .otherwise('3+ Months (90+ days)')
    )

# Join with contacts
payables_with_contact = payables_df \
    .join(
        dim_contact.select('contact_id', 'contact_key'),
        'contact_id',
        'left'
    )

# Get next payable_key
max_key = spark.sql(f"SELECT COALESCE(MAX(payable_key), 0) as max_key FROM {gold_schema}.fact_payables").collect()[0]['max_key']

# Prepare final dataset
payables_final = payables_with_contact \
    .withColumn('payable_key', monotonically_increasing_id() + max_key + 1) \
    .withColumn('created_date', current_timestamp()) \
    .select(
        'payable_key', 'invoice_date_key', 'due_date_key', 'snapshot_date_key',
        'contact_key', 'bill_id', 'bill_number', 'total_amount', 'amount_paid',
        'amount_due', 'days_overdue', 'aging_bucket', 'bill_status',
        'currency_code', 'tenant_id', 'created_date'
    )

# Write to Delta
payables_final.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable(f"{gold_schema}.fact_payables")
# display(payables_final)
payables_count = payables_final.count()
total_ap = payables_final.agg(sum('amount_due')).collect()[0][0]

print(f"✓ Payables snapshot: {payables_count} bills")
print(f"  Total outstanding: ${total_ap:,.2f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

# MAGIC %%sql
# MAGIC -- Receivables by aging bucket
# MAGIC SELECT 
# MAGIC     aging_bucket,
# MAGIC     COUNT(*) as invoice_count,
# MAGIC     SUM(amount_due) as total_outstanding
# MAGIC FROM gold.fact_receivables
# MAGIC WHERE snapshot_date_key = {snapshot_date_key}
# MAGIC GROUP BY aging_bucket
# MAGIC ORDER BY 
# MAGIC     CASE aging_bucket
# MAGIC         WHEN 'Current (0-30 days)' THEN 1
# MAGIC         WHEN '1 Month (31-60 days)' THEN 2
# MAGIC         WHEN '2 Months (61-90 days)' THEN 3
# MAGIC         WHEN '3+ Months (90+ days)' THEN 4
# MAGIC     END

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Payables by aging bucket
# MAGIC SELECT 
# MAGIC     aging_bucket,
# MAGIC     COUNT(*) as bill_count,
# MAGIC     SUM(amount_due) as total_outstanding
# MAGIC FROM gold.fact_payables
# MAGIC WHERE snapshot_date_key = {snapshot_date_key}
# MAGIC GROUP BY aging_bucket
# MAGIC ORDER BY 
# MAGIC     CASE aging_bucket
# MAGIC         WHEN 'Not Yet Due' THEN 0
# MAGIC         WHEN 'Current (0-30 days)' THEN 1
# MAGIC         WHEN '1 Month (31-60 days)' THEN 2
# MAGIC         WHEN '2 Months (61-90 days)' THEN 3
# MAGIC         WHEN '3+ Months (90+ days)' THEN 4
# MAGIC     END

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

runtime = time.time() - start_time
print(f"\n✅ AR/AP Snapshots completed in {runtime:.2f} seconds")
print(f"\n📊 Summary:")
print(f"   Receivables: {receivables_count} invoices, ${total_ar:,.2f} outstanding")
print(f"   Payables: {payables_count} bills, ${total_ap:,.2f} outstanding")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
