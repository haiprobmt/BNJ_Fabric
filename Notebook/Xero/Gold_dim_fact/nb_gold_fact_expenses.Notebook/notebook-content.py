# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

batch_id = 20260303112233
job_id = '9909'
src_table = 'brz_bank_transactions'
src_catalog = 'xero'
tgt_table = 'fact_expenses'
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

def create_fact_expenses():
    # Read bank transactions
    bank_txn = spark.table(f"{BRONZE_SCHEMA}.{src_catalog}.{src_table}")
    # Read contact dimension for joins
    dim_contact = spark.table(f"{GOLD_SCHEMA}.{tgt_catalog}.dim_contact").where(col('is_active') == True)
    # Read invoices
    invoices = spark.table(f"{BRONZE_SCHEMA}.{src_catalog}.brz_invoices") 

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


    # Union all expenses
    all_expenses = expenses_with_contact.union(expenses_invoices_with_contact)

    # Get next expense_key
    # max_key = spark.sql(f"SELECT COALESCE(MAX(expense_key), 0) as max_key FROM {GOLD_SCHEMA}.{tgt_catalog}.{tgt_table}").collect()[0]['max_key']

    target_table = f"{GOLD_SCHEMA}.{tgt_catalog}.{tgt_table}"

    # Get next expense_key safely
    if spark.catalog.tableExists(target_table):
        max_key = spark.sql(f"""
            SELECT COALESCE(MAX(expense_key), 0) AS max_key
            FROM {target_table}
        """).collect()[0]["max_key"]
    else:
        max_key = 0

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

    return expenses_final



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    target_table = f'{GOLD_SCHEMA}.{tgt_catalog}.{tgt_table}'
    print(target_table)
    df = create_fact_expenses()
    src_cnt = 0
    tgt_cnt = df.count()
    
    # Create empty table first only if missing
    if not spark.catalog.tableExists(target_table):
        df.limit(0).write.format("delta").saveAsTable(target_table)
        print(f"✅ Created empty table {target_table}")

    df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created fact_expenses", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print(f"✅ Created {target_table} with {tgt_cnt} rows")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed fact_expenses. {safe_exception_text(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
