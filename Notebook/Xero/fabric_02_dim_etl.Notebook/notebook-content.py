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

# # Dimension ETL - Contact & Account
# 
# **Purpose:** Load customer and supplier dimension with SCD Type 2
# 
# **Runtime:** ~3 minutes
# 
# **Schedule:** Daily
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
print(f"Starting dimension ETL at {datetime.now()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Extract Contacts from Xero

# CELL ********************

# Read invoices and extract contacts
invoices = spark.table(f"{bronze_schema}.invoices")

contacts_from_invoices = invoices.select(
    col("`records.contact.contact_id`").alias('contact_id'),
    col("`records.contact.name`").alias('contact_name'),
    col("`records.contact.is_customer`").cast('boolean').alias('is_customer'),
    col("`records.contact.is_supplier`").cast('boolean').alias('is_supplier'),
    col("`records.contact.email_address`").alias('email'),
    col("`records.contact.account_number`").alias('account_number'),
    col("`records.contact.tax_number`").alias('tax_number'),
    col("`records.contact.default_currency`").alias('default_currency'),
    col("`records.contact.updated_date_utc`").cast('timestamp').alias('updated_date')
).where(
    col('contact_id').isNotNull()
).distinct()

# Read bank transactions and extract contacts
bank_txn = spark.table(f"{bronze_schema}.bank_transactions")

contacts_from_bank = bank_txn.select(
    col("`records.contact.contact_id`").alias('contact_id'),
    col("`records.contact.name`").alias('contact_name'),
    col("`records.contact.is_customer`").cast('boolean').alias('is_customer'),
    col("`records.contact.is_supplier`").cast('boolean').alias('is_supplier'),
    col("`records.contact.email_address`").alias('email'),
    col("`records.contact.account_number`").alias('account_number'),
    col("`records.contact.tax_number`").alias('tax_number'),
    col("`records.contact.default_currency`").alias('default_currency'),
    col("`records.contact.updated_date_utc`").cast('timestamp').alias('updated_date')
).where(
    col('contact_id').isNotNull()
).distinct()

# Union all contacts
all_contacts = contacts_from_invoices.union(contacts_from_bank).distinct()

# Determine contact type
contacts_df = all_contacts.withColumn(
    'contact_type',
    when(col('is_customer') & col('is_supplier'), 'Both')
    .when(col('is_customer'), 'Customer')
    .when(col('is_supplier'), 'Supplier')
    .otherwise('Unknown')
)

print(f"Extracted {contacts_df.count()} unique contacts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Load Contact Dimension (SCD Type 2)

# CELL ********************

from delta.tables import DeltaTable

# Read existing dimension
dim_contact_exists = spark.catalog.tableExists(f"{gold_schema}.dim_contact")

if dim_contact_exists:
    dim_contact = DeltaTable.forName(spark, f"{gold_schema}.dim_contact")
    existing_df = dim_contact.toDF().where(col('is_current') == True)
    
    # Find changed records
    changed_contacts = contacts_df.alias('source') \
        .join(
            existing_df.alias('target'),
            (col('source.contact_id') == col('target.contact_id')) & 
            (col('target.is_current') == True),
            'inner'
        ) \
        .where(
            (col('source.contact_name') != col('target.contact_name')) |
            (coalesce(col('source.email'), lit('')) != coalesce(col('target.email'), lit(''))) |
            (col('source.contact_type') != col('target.contact_type'))
        ) \
        .select('source.*')
    
    # Expire changed records
    if changed_contacts.count() > 0:
        expired_contacts = changed_contacts.select('contact_id').collect()
        expired_ids = [row['contact_id'] for row in expired_contacts]
        
        dim_contact.update(
            condition = (col('contact_id').isin(expired_ids)) & (col('is_current') == True),
            set = {
                'effective_to': lit(current_timestamp()),
                'is_current': lit(False),
                'updated_date': lit(current_timestamp())
            }
        )
        print(f"Expired {len(expired_ids)} changed contacts")
    
    # Find new contacts
    new_contacts = contacts_df.alias('source') \
        .join(
            existing_df.alias('target'),
            col('source.contact_id') == col('target.contact_id'),
            'left_anti'
        )
    
    # Union new and changed contacts
    contacts_to_insert = new_contacts.union(changed_contacts)
    
else:
    # First load - all contacts are new
    contacts_to_insert = contacts_df

# Prepare records for insert
if contacts_to_insert.count() > 0:
    # Get next contact_key
    if dim_contact_exists:
        max_key = spark.sql(f"SELECT COALESCE(MAX(contact_key), 0) as max_key FROM {gold_schema}.dim_contact").collect()[0]['max_key']
    else:
        max_key = 0
    
    new_dimension_records = contacts_to_insert \
        .withColumn('contact_key', monotonically_increasing_id() + max_key + 1) \
        .withColumn('phone', lit(None).cast('string')) \
        .withColumn('payment_terms', lit(None).cast('string')) \
        .withColumn('default_currency', coalesce(col('default_currency'), lit('SGD'))) \
        .withColumn('effective_from', current_timestamp()) \
        .withColumn('effective_to', lit(None).cast('timestamp')) \
        .withColumn('is_current', lit(True)) \
        .withColumn('source_system', lit('Xero')) \
        .withColumn('created_date', current_timestamp()) \
        .withColumn('updated_date', current_timestamp()) \
        .select(
            'contact_key', 'contact_id', 'contact_name', 'contact_type',
            'is_customer', 'is_supplier', 'email', 'phone', 'account_number',
            'tax_number', 'default_currency', 'payment_terms',
            'effective_from', 'effective_to', 'is_current',
            'source_system', 'created_date', 'updated_date'
        )
    
    # Append to dimension table
    new_dimension_records.write \
        .format('delta') \
        .mode('append') \
        .saveAsTable(f"{gold_schema}.dim_contact")
    
    print(f"✓ Inserted {new_dimension_records.count()} contact records")
else:
    print("No new or changed contacts to insert")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Load Account Dimension

# CELL ********************

# Read accounts from Xero
accounts = spark.table(f"{bronze_schema}.accounts")

accounts_df = accounts.select(
    col("`records.account_id`").alias('account_id'),
    col("`records.code`").alias('account_code'),
    col("`records.name`").alias('account_name'),
    col("`records.type`").alias('account_type'),
    col("`records.tax_type`").alias('tax_type'),
    when(col("`records.status`") == 'ACTIVE', True).otherwise(False).alias('is_active')
).where(
    col('account_id').isNotNull()
).distinct()

# Add account class
accounts_df = accounts_df.withColumn(
    'account_class',
    when(col('account_type').isin(['BANK', 'CURRENT', 'FIXED', 'INVENTORY']), 'ASSET')
    .when(col('account_type').isin(['CURRLIAB', 'TERMLIAB']), 'LIABILITY')
    .when(col('account_type') == 'EQUITY', 'EQUITY')
    .when(col('account_type').isin(['EXPENSE', 'DIRECTCOSTS', 'OVERHEADS']), 'EXPENSE')
    .when(col('account_type').isin(['REVENUE', 'OTHERINCOME']), 'REVENUE')
    .otherwise('OTHER')
)

# Get next account_key
if spark.catalog.tableExists(f"{gold_schema}.dim_account"):
    max_key = spark.sql(f"SELECT COALESCE(MAX(account_key), 0) as max_key FROM {gold_schema}.dim_account").collect()[0]['max_key']
else:
    max_key = 0

# Prepare for insert
accounts_final = accounts_df \
    .withColumn('account_key', monotonically_increasing_id() + max_key + 1) \
    .withColumn('source_system', lit('Xero')) \
    .withColumn('created_date', current_timestamp()) \
    .withColumn('updated_date', current_timestamp()) \
    .select(
        'account_key', 'account_id', 'account_code', 'account_name',
        'account_type', 'account_class', 'tax_type', 'is_active',
        'source_system', 'created_date', 'updated_date'
    )

# Overwrite (full refresh for accounts)
accounts_final.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{gold_schema}.dim_account")

print(f"✓ Loaded {accounts_final.count()} accounts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC     contact_type,
# MAGIC     COUNT(*) as total_records,
# MAGIC     SUM(CAST(is_current AS INT)) as current_records
# MAGIC FROM gold.dim_contact
# MAGIC GROUP BY contact_type

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC     account_class,
# MAGIC     COUNT(*) as account_count
# MAGIC FROM gold.dim_account
# MAGIC WHERE is_active = true
# MAGIC GROUP BY account_class

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

runtime = time.time() - start_time
print(f"\n✅ Dimension ETL completed in {runtime:.2f} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
