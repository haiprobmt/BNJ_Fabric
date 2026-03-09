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

# # Fact ETL - Revenue & Expenses
# 
# **Purpose:** Load revenue and expense transactions
# 
# **Runtime:** ~10 minutes
# 
# **Schedule:** Daily
# 
# **Refresh Strategy:** Rolling 90 days
# 
# ---

# MARKDOWN ********************

# ## Configuration

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import time

# Configuration
bronze_schema = "lh_bnj_bronze.xero"
gold_schema = "lh_bnj_gold.gold"
lookback_days = 90  # Rolling window

start_time = time.time()
cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
cutoff_date_key = int((datetime.now() - timedelta(days=lookback_days)).strftime('%Y%m%d'))

print(f"Starting fact ETL at {datetime.now()}")
print(f"Processing data from {cutoff_date} onwards")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 1: Revenue Fact ETL

# MARKDOWN ********************

# ### Delete recent records (for reprocessing)

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM gold.fact_revenue
# MAGIC WHERE invoice_date_key >= {cutoff_date_key}

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Extract and transform revenue data

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold.fact_revenue;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_gold.gold.fact_revenue (
# MAGIC     revenue_key BIGINT,
# MAGIC     invoice_date_key INT,
# MAGIC     due_date_key INT,
# MAGIC     contact_key BIGINT,
# MAGIC     account_key BIGINT,
# MAGIC     invoice_id STRING,
# MAGIC     invoice_number STRING,
# MAGIC     line_item_id STRING,
# MAGIC     line_amount DECIMAL(18,2),
# MAGIC     tax_amount DECIMAL(18,2),
# MAGIC     total_amount DECIMAL(18,2),
# MAGIC     amount_paid DECIMAL(18,2),
# MAGIC     amount_due DECIMAL(18,2),
# MAGIC     invoice_status STRING,
# MAGIC     currency_code STRING,
# MAGIC     item_description STRING,
# MAGIC     tenant_id STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     updated_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (invoice_date_key);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read invoices
invoices = spark.table(f"{bronze_schema}.invoices")

# Read contact dimension for joins
dim_contact = spark.table(f"{gold_schema}.dim_contact").where(col('is_current') == True)

# Extract revenue invoices (ACCREC)
revenue_df = invoices \
    .where(col("`records.type`") == 'ACCREC') \
    .where(col("`records.status`") != 'VOIDED') \
    .select(
        date_format(to_date(col("`records.date`")), 'yyyyMMdd').cast('int').alias('invoice_date_key'),
        date_format(to_date(col("`records.due_date`")), 'yyyyMMdd').cast('int').alias('due_date_key'),
        col("`records.contact.contact_id`").alias('contact_id'),
        col("`records.invoice_id`").alias('invoice_id'),
        col("`records.invoice_number`").alias('invoice_number'),
        col("`records.sub_total`").cast('decimal(18,2)').alias('line_amount'),
        col("`records.total_tax`").cast('decimal(18,2)').alias('tax_amount'),
        col("`records.total`").cast('decimal(18,2)').alias('total_amount'),
        col("`records.amount_paid`").cast('decimal(18,2)').alias('amount_paid'),
        col("`records.amount_due`").cast('decimal(18,2)').alias('amount_due'),
        col("`records.status`").alias('invoice_status'),
        coalesce(col("`records.currency_code`"), lit('SGD')).alias('currency_code'),
        col("`records.reference`").alias('item_description'),
        col('tenant_id')
    )

# Join with contact dimension
revenue_with_contact = revenue_df \
    .join(
        dim_contact.select('contact_id', 'contact_key'),
        'contact_id',
        'left'
    )

# Get next revenue_key
max_key = spark.sql(f"SELECT COALESCE(MAX(revenue_key), 0) as max_key FROM {gold_schema}.fact_revenue").collect()[0]['max_key']

# Prepare final dataset
revenue_final = revenue_with_contact \
    .withColumn('revenue_key', monotonically_increasing_id() + max_key + 1) \
    .withColumn('account_key', lit(None).cast('bigint')) \
    .withColumn('line_item_id', lit(None).cast('string')) \
    .withColumn('created_date', current_timestamp()) \
    .withColumn('updated_date', current_timestamp()) \
    .select(
        'revenue_key', 'invoice_date_key', 'due_date_key', 'contact_key', 'account_key',
        'invoice_id', 'invoice_number', 'line_item_id', 'line_amount', 'tax_amount',
        'total_amount', 'amount_paid', 'amount_due', 'invoice_status', 'currency_code',
        'item_description', 'tenant_id', 'created_date', 'updated_date'
    )

# Write to Delta
revenue_final.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable(f"{gold_schema}.fact_revenue")
# display(revenue_final)
revenue_count = revenue_final.count()
print(f"✓ Loaded {revenue_count} revenue records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 2: Expenses Fact ETL

# MARKDOWN ********************

# ### Delete recent records

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM gold.fact_expenses
# MAGIC WHERE transaction_date_key >= {cutoff_date_key}

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load expenses from bank transactions

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold.fact_expenses;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_gold.gold.fact_expenses (
# MAGIC     expense_key BIGINT,
# MAGIC     transaction_date_key INT,
# MAGIC     contact_key BIGINT,
# MAGIC     account_key BIGINT,
# MAGIC     transaction_id STRING,
# MAGIC     transaction_type STRING,
# MAGIC     amount DECIMAL(18,2),
# MAGIC     tax_amount DECIMAL(18,2),
# MAGIC     expense_category STRING,
# MAGIC     reference STRING,
# MAGIC     description STRING,
# MAGIC     currency_code STRING,
# MAGIC     tenant_id STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     updated_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (transaction_date_key);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read bank transactions
bank_txn = spark.table(f"{bronze_schema}.bank_transactions")

# Extract expense transactions
expenses_bank = bank_txn \
    .where(col("`records.type`").isin(['SPEND', 'SPEND-OVERPAYMENT'])) \
    .where(col("`records.status`") != 'DELETED') \
    .select(
        date_format(to_date(col("`records.date`")), 'yyyyMMdd').cast('int').alias('transaction_date_key'),
        col("`records.contact.contact_id`").alias('contact_id'),
        col("`records.bank_transaction_id`").alias('transaction_id'),
        col("`records.type`").alias('transaction_type'),
        col("`records.total`").cast('decimal(18,2)').alias('amount'),
        col("`records.total_tax`").cast('decimal(18,2)').alias('tax_amount'),
        col("`records.reference`").alias('reference'),
        coalesce(col("`records.currency_code`"), lit('SGD')).alias('currency_code'),
        col('tenant_id')
    )

# Categorize expenses
expenses_bank = expenses_bank.withColumn(
    'expense_category',
    when(lower(col('reference')).contains('salary') | lower(col('reference')).contains('payroll'), 'Salaries')
    .when(lower(col('reference')).contains('rent') | lower(col('reference')).contains('lease'), 'Rent')
    .when(lower(col('reference')).contains('marketing') | lower(col('reference')).contains('advertising'), 'Marketing')
    .when(lower(col('reference')).contains('utilities') | lower(col('reference')).contains('electricity') | lower(col('reference')).contains('water'), 'Utilities')
    .when(lower(col('reference')).contains('insurance'), 'Insurance')
    .when(lower(col('reference')).contains('office') | lower(col('reference')).contains('supplies'), 'Office Supplies')
    .when(lower(col('reference')).contains('inventory') | lower(col('reference')).contains('stock'), 'Cost of Goods Sold')
    .otherwise('Miscellaneous')
)

# Join with contacts
expenses_with_contact = expenses_bank \
    .join(
        dim_contact.select('contact_id', 'contact_key'),
        'contact_id',
        'left'
    )

print(f"Extracted {expenses_with_contact.count()} expenses from bank transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load expenses from supplier invoices (ACCPAY)

# CELL ********************

# Extract supplier invoices
expenses_invoices = invoices \
    .where(col("`records.type`") == 'ACCPAY') \
    .where(col("`records.status`").isin(['AUTHORISED', 'PAID'])) \
    .select(
        date_format(to_date(col("`records.date`")), 'yyyyMMdd').cast('int').alias('transaction_date_key'),
        col("`records.contact.contact_id`").alias('contact_id'),
        col("`records.invoice_id`").alias('transaction_id'),
        lit('ACCPAY').alias('transaction_type'),
        col("`records.total`").cast('decimal(18,2)').alias('amount'),
        col("`records.total_tax`").cast('decimal(18,2)').alias('tax_amount'),
        col("`records.reference`").alias('reference'),
        coalesce(col("`records.currency_code`"), lit('SGD')).alias('currency_code'),
        col('tenant_id')
    ) \
    .withColumn('expense_category', lit('Accounts Payable'))

# Join with contacts
expenses_invoices_with_contact = expenses_invoices \
    .join(
        dim_contact.select('contact_id', 'contact_key'),
        'contact_id',
        'left'
    )

print(f"Extracted {expenses_invoices_with_contact.count()} expenses from supplier invoices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Combine and load expenses

# CELL ********************

# Union all expenses
all_expenses = expenses_with_contact.union(expenses_invoices_with_contact)

# Get next expense_key
max_key = spark.sql(f"SELECT COALESCE(MAX(expense_key), 0) as max_key FROM {gold_schema}.fact_expenses").collect()[0]['max_key']

# Prepare final dataset
expenses_final = all_expenses \
    .withColumn('expense_key', monotonically_increasing_id() + max_key + 1) \
    .withColumn('account_key', lit(None).cast('bigint')) \
    .withColumn('description', lit(None).cast('string')) \
    .withColumn('created_date', current_timestamp()) \
    .withColumn('updated_date', current_timestamp()) \
    .select(
        'expense_key', 'transaction_date_key', 'contact_key', 'account_key',
        'transaction_id', 'transaction_type', 'amount', 'tax_amount',
        'expense_category', 'reference', 'description', 'currency_code',
        'tenant_id', 'created_date', 'updated_date'
    )

# Write to Delta
expenses_final.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable(f"{gold_schema}.fact_expenses")

expenses_count = expenses_final.count()
print(f"✓ Loaded {expenses_count} expense records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

# MAGIC %%sql
# MAGIC -- Revenue summary
# MAGIC SELECT 
# MAGIC     invoice_status,
# MAGIC     COUNT(*) as invoice_count,
# MAGIC     SUM(total_amount) as total_revenue,
# MAGIC     SUM(amount_due) as outstanding
# MAGIC FROM gold.fact_revenue
# MAGIC WHERE invoice_date_key >= {cutoff_date_key}
# MAGIC GROUP BY invoice_status

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Expense summary
# MAGIC SELECT 
# MAGIC     expense_category,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(amount) as total_amount
# MAGIC FROM gold.fact_expenses
# MAGIC WHERE transaction_date_key >= {cutoff_date_key}
# MAGIC GROUP BY expense_category
# MAGIC ORDER BY total_amount DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

runtime = time.time() - start_time
print(f"\n✅ Revenue & Expenses ETL completed in {runtime:.2f} seconds")
print(f"   Revenue records: {revenue_count}")
print(f"   Expense records: {expenses_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
