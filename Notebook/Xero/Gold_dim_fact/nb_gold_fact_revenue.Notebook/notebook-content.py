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

# PARAMETERS CELL ********************

batch_id = 20260210115427
job_id = '3704'
src_table = 'brz_invoices'
src_catalog = 'xero'
tgt_table = 'fact_revenue'
tgt_catalog = 'gold_test'
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

BRONZE_SCHEMA = f'`{WORKSPACE_NAME}`.lh_bnj_bronze'
GOLD_SCHEMA = f'`{WORKSPACE_NAME}`.lh_bnj_gold'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_fact_revenue():
    # Read contact dimension for joins
    dim_contact = spark.table(f"{GOLD_SCHEMA}.{tgt_catalog}.dim_contact").where(col('is_active') == True)
    # Read invoices
    invoices = spark.table(f"{BRONZE_SCHEMA}.{src_catalog}.brz_invoices") 

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
    # max_key = spark.sql(f"SELECT COALESCE(MAX(revenue_key), 0) as max_key FROM {GOLD_SCHEMA}.{tgt_catalog}.fact_revenue").collect()[0]['max_key']

    target_table = f"{GOLD_SCHEMA}.{tgt_catalog}.fact_revenue"

    # Get next expense_key safely
    if spark.catalog.tableExists(target_table):
        max_key = spark.sql(f"""
            SELECT COALESCE(MAX(expense_key), 0) AS max_key
            FROM {target_table}
        """).collect()[0]["max_key"]
    else:
        max_key = 0

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

    return revenue_final

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    target_table = f'{GOLD_SCHEMA}.{tgt_catalog}.{tgt_table}'
    print(target_table)
    df = create_fact_revenue()
    src_cnt = 0
    tgt_cnt = df.count()
    
    # Create empty table first only if missing
    if not spark.catalog.tableExists(target_table):
        df.limit(0).write.format("delta").saveAsTable(target_table)
        print(f"✅ Created empty table {target_table}")
        
    #log_data_quality("dim_location", df, "location_key")
    df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created fact_revenue", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print(f"✅ Created {target_table} with {tgt_cnt} rows")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed fact_revenue. {safe_exception_text(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
