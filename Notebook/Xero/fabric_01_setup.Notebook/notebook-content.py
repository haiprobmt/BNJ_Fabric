# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Gold Layer Setup - Schema & Date Dimension
# 
# **Purpose:** One-time setup to create Gold layer schema and populate date dimension
# 
# **Runtime:** ~5 minutes
# 
# **Run Frequency:** One-time (or when extending date range)
# 
# ---
# 
# ## Steps:
# 1. Create gold and rpt schemas
# 2. Create dimension tables
# 3. Create fact tables
# 4. Create ETL control tables
# 5. Populate date dimension
# 
# ---

# MARKDOWN ********************

# ## Configuration

# CELL ********************

# Database configuration
database_name = "your_database_name"  # Update this
gold_schema = "gold"
rpt_schema = "rpt"

print(f"Database: {database_name}")
print(f"Gold schema: {gold_schema}")
print(f"Report schema: {rpt_schema}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Create Schemas

# CELL ********************

# MAGIC %%sql
# MAGIC -- Create gold schema if not exists
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC 
# MAGIC -- Create rpt schema for reporting views
# MAGIC CREATE SCHEMA IF NOT EXISTS rpt;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Create Date Dimension

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS gold.dim_date;
# MAGIC 
# MAGIC CREATE TABLE gold.dim_date (
# MAGIC     date_key INT,
# MAGIC     full_date DATE,
# MAGIC     year INT,
# MAGIC     quarter INT,
# MAGIC     month INT,
# MAGIC     month_name STRING,
# MAGIC     month_short STRING,
# MAGIC     week_of_year INT,
# MAGIC     day_of_month INT,
# MAGIC     day_of_week INT,
# MAGIC     day_name STRING,
# MAGIC     day_short STRING,
# MAGIC     is_weekend BOOLEAN,
# MAGIC     is_holiday BOOLEAN,
# MAGIC     holiday_name STRING,
# MAGIC     fiscal_year INT,
# MAGIC     fiscal_quarter INT,
# MAGIC     fiscal_month INT,
# MAGIC     year_month STRING,
# MAGIC     quarter_name STRING
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Create Contact Dimension

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS gold.dim_contact;
# MAGIC 
# MAGIC CREATE TABLE gold.dim_contact (
# MAGIC     contact_key BIGINT,
# MAGIC     contact_id STRING,
# MAGIC     contact_name STRING,
# MAGIC     contact_type STRING,
# MAGIC     is_customer BOOLEAN,
# MAGIC     is_supplier BOOLEAN,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     account_number STRING,
# MAGIC     tax_number STRING,
# MAGIC     default_currency STRING,
# MAGIC     payment_terms STRING,
# MAGIC     effective_from TIMESTAMP,
# MAGIC     effective_to TIMESTAMP,
# MAGIC     is_current BOOLEAN,
# MAGIC     source_system STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     updated_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Create Account Dimension

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS gold.dim_account;
# MAGIC 
# MAGIC CREATE TABLE gold.dim_account (
# MAGIC     account_key BIGINT,
# MAGIC     account_id STRING,
# MAGIC     account_code STRING,
# MAGIC     account_name STRING,
# MAGIC     account_type STRING,
# MAGIC     account_class STRING,
# MAGIC     tax_type STRING,
# MAGIC     is_active BOOLEAN,
# MAGIC     source_system STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     updated_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Create Revenue Fact Table

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS gold.fact_revenue;
# MAGIC 
# MAGIC CREATE TABLE gold.fact_revenue (
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

# MARKDOWN ********************

# ## Step 6: Create Expenses Fact Table

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS gold.fact_expenses;
# MAGIC 
# MAGIC CREATE TABLE gold.fact_expenses (
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

# MARKDOWN ********************

# ## Step 7: Create Receivables Fact Table

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS gold.fact_receivables;
# MAGIC 
# MAGIC CREATE TABLE gold.fact_receivables (
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

# MARKDOWN ********************

# ## Step 8: Create Payables Fact Table

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS gold.fact_payables;
# MAGIC 
# MAGIC CREATE TABLE gold.fact_payables (
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

# MARKDOWN ********************

# ## Step 9: Create ETL Control Tables

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS gold.etl_control;
# MAGIC 
# MAGIC CREATE TABLE gold.etl_control (
# MAGIC     control_id BIGINT,
# MAGIC     table_name STRING,
# MAGIC     last_run_date TIMESTAMP,
# MAGIC     last_success_date TIMESTAMP,
# MAGIC     rows_processed BIGINT,
# MAGIC     rows_inserted BIGINT,
# MAGIC     rows_updated BIGINT,
# MAGIC     rows_deleted BIGINT,
# MAGIC     status STRING,
# MAGIC     error_message STRING,
# MAGIC     runtime_seconds INT,
# MAGIC     created_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS gold.data_quality_log;
# MAGIC 
# MAGIC CREATE TABLE gold.data_quality_log (
# MAGIC     log_id BIGINT,
# MAGIC     check_name STRING,
# MAGIC     table_name STRING,
# MAGIC     check_date TIMESTAMP,
# MAGIC     records_checked BIGINT,
# MAGIC     records_failed BIGINT,
# MAGIC     failure_rate DECIMAL(5,2),
# MAGIC     passed BOOLEAN,
# MAGIC     details STRING,
# MAGIC     severity STRING
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 10: Populate Date Dimension

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import pandas as pd

# Generate date range
start_date = datetime(2020, 1, 1)
end_date = datetime(2030, 12, 31)

# Create date range
date_range = pd.date_range(start=start_date, end=end_date, freq='D')

# Create DataFrame
dates_df = spark.createDataFrame(
    [(d,) for d in date_range],
    ['full_date']
)

# Add date dimension attributes
dates_df = dates_df.select(
    date_format('full_date', 'yyyyMMdd').cast('int').alias('date_key'),
    col('full_date'),
    year('full_date').alias('year'),
    quarter('full_date').alias('quarter'),
    month('full_date').alias('month'),
    date_format('full_date', 'MMMM').alias('month_name'),
    date_format('full_date', 'MMM').alias('month_short'),
    weekofyear('full_date').alias('week_of_year'),
    dayofmonth('full_date').alias('day_of_month'),
    dayofweek('full_date').alias('day_of_week'),
    date_format('full_date', 'EEEE').alias('day_name'),
    date_format('full_date', 'EEE').alias('day_short'),
    when(dayofweek('full_date').isin([1, 7]), True).otherwise(False).alias('is_weekend'),
    lit(False).alias('is_holiday'),
    lit(None).cast('string').alias('holiday_name'),
    year('full_date').alias('fiscal_year'),
    quarter('full_date').alias('fiscal_quarter'),
    month('full_date').alias('fiscal_month'),
    date_format('full_date', 'yyyy-MM').alias('year_month'),
    concat(lit('Q'), quarter('full_date'), lit(' '), year('full_date')).alias('quarter_name')
)

# Write to Delta table
dates_df.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('gold.dim_date')

print(f"✓ Date dimension populated with {dates_df.count()} rows")
print(f"  Date range: {start_date.date()} to {end_date.date()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 11: Add Holidays (Singapore)

# CELL ********************

# MAGIC %%sql
# MAGIC -- Update for New Year's Day
# MAGIC UPDATE gold.dim_date
# MAGIC SET is_holiday = true, holiday_name = "New Year's Day"
# MAGIC WHERE day_of_month = 1 AND month = 1;
# MAGIC 
# MAGIC -- Update for National Day
# MAGIC UPDATE gold.dim_date
# MAGIC SET is_holiday = true, holiday_name = 'National Day'
# MAGIC WHERE day_of_month = 9 AND month = 8;
# MAGIC 
# MAGIC -- Update for Christmas
# MAGIC UPDATE gold.dim_date
# MAGIC SET is_holiday = true, holiday_name = 'Christmas Day'
# MAGIC WHERE day_of_month = 25 AND month = 12;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_dates,
# MAGIC     MIN(full_date) as earliest_date,
# MAGIC     MAX(full_date) as latest_date,
# MAGIC     SUM(CAST(is_weekend AS INT)) as weekend_days,
# MAGIC     SUM(CAST(is_holiday AS INT)) as holidays
# MAGIC FROM gold.dim_date

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Show sample dates
# MAGIC SELECT * FROM gold.dim_date
# MAGIC WHERE year = 2025 AND month = 1
# MAGIC ORDER BY full_date
# MAGIC LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✅ Setup Complete!
# 
# **Next Steps:**
# 1. Run dimension ETL notebooks (contacts, accounts)
# 2. Run fact ETL notebooks (revenue, expenses, receivables, payables)
# 3. Create reporting views
# 4. Connect to dashboard
