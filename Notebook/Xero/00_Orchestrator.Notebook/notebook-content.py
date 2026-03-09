# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Xero to Fabric Lakehouse - Orchestrator
# # MAGIC
# This is the main orchestrator notebook that runs the complete end-to-end Xero data extraction pipeline.
# # MAGIC
# ## Pipeline Flow:
# 1. **Config & Setup** - Initialize parameters and create folder structure
# 2. **Authentication** - Connect to Xero API with OAuth2
# 3. **Extract to Bronze** - Extract all data types and save as Delta tables
# 4. **Validation** - Validate extraction results
# # MAGIC
# ## Parameters:
# - `lakehouse_name`: Target lakehouse name
# - `client_id`: Xero OAuth2 Client ID
# - `client_secret`: Xero OAuth2 Client Secret

# MARKDOWN ********************

# ## 1. Set Parameters

# CELL ********************

# Create widgets for parameters
dbutils.widgets.text("lakehouse_name", "xero_lakehouse", "1. Lakehouse Name")
dbutils.widgets.text("bronze_path", "/lakehouse/default/Files/bronze/xero", "2. Bronze Path")
dbutils.widgets.text("client_id", "", "3. Xero Client ID")
dbutils.widgets.text("client_secret", "", "4. Xero Client Secret")
dbutils.widgets.text("token_file_path", "/lakehouse/default/Files/config/xero_token.json", "5. Token File Path")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Validate Parameters

# CELL ********************

from datetime import datetime

# Get parameters
params = {
    'lakehouse_name': dbutils.widgets.get("lakehouse_name"),
    'bronze_path': dbutils.widgets.get("bronze_path"),
    'client_id': dbutils.widgets.get("client_id"),
    'client_secret': dbutils.widgets.get("client_secret"),
    'token_file_path': dbutils.widgets.get("token_file_path"),
    'execution_time': datetime.utcnow().isoformat()
}

# Validate required parameters
if not params['client_id'] or not params['client_secret']:
    raise ValueError("Xero Client ID and Client Secret are required!")

print("Pipeline Parameters:")
print("="*70)
for key, value in params.items():
    if key in ['client_id', 'client_secret']:
        print(f"{key}: {'*' * 10}")
    else:
        print(f"{key}: {value}")
print("="*70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Run Config and Setup

# CELL ********************

print("\n" + "="*70)
print("STEP 1: Configuration and Setup")
print("="*70)

config_result = dbutils.notebook.run(
    "./01_Config_and_Setup",
    timeout_seconds=300,
    arguments={
        "lakehouse_name": params['lakehouse_name'],
        "bronze_path": params['bronze_path'],
        "client_id": params['client_id'],
        "client_secret": params['client_secret'],
        "token_file_path": params['token_file_path']
    }
)

print(f"Config Result: {config_result}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Authenticate with Xero

# CELL ********************

print("\n" + "="*70)
print("STEP 2: Xero Authentication")
print("="*70)

auth_result = dbutils.notebook.run(
    "./02_Xero_Authentication",
    timeout_seconds=300,
    arguments={
        "client_id": params['client_id'],
        "client_secret": params['client_secret'],
        "token_file_path": params['token_file_path']
    }
)

print(f"Authentication Result: {auth_result}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Extract Data to Bronze Layer

# CELL ********************

print("\n" + "="*70)
print("STEP 3: Extract to Bronze Layer")
print("="*70)

import json

extraction_result = dbutils.notebook.run(
    "./03_Extract_to_Bronze",
    timeout_seconds=3600,  # 1 hour timeout for large extractions
    arguments={
        "bronze_path": params['bronze_path'],
        "client_id": params['client_id'],
        "client_secret": params['client_secret'],
        "token_file_path": params['token_file_path']
    }
)

extraction_data = json.loads(extraction_result)
print(f"\nExtraction Summary:")
print(f"  Total Records: {extraction_data['total_records']:,}")
print(f"  Data Types: {extraction_data['data_types']}")
print(f"  Errors: {extraction_data['errors']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Pipeline Summary

# CELL ********************

print("\n" + "="*70)
print("PIPELINE EXECUTION SUMMARY")
print("="*70)

summary = {
    'pipeline': 'Xero to Fabric Lakehouse',
    'execution_time': params['execution_time'],
    'status': 'SUCCESS' if extraction_data['errors'] == 0 else 'COMPLETED_WITH_ERRORS',
    'lakehouse': params['lakehouse_name'],
    'bronze_path': params['bronze_path'],
    'total_records_extracted': extraction_data['total_records'],
    'data_types_extracted': extraction_data['data_types'],
    'errors_count': extraction_data['errors']
}

print(json.dumps(summary, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Query Bronze Tables

# CELL ********************

# Show available bronze tables
print("\nAvailable Bronze Tables:")
print("="*70)

tables = spark.sql("SHOW TABLES LIKE 'bronze_xero_*'").collect()
for table in tables:
    table_name = table['tableName']
    count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
    print(f"{table_name:.<50} {count:>10,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Sample Data Preview

# CELL ********************

# Show sample from invoices table
print("\nSample Data - Bronze Invoices (first 5 records):")
print("="*70)

try:
    display(spark.sql("SELECT * FROM bronze_xero_invoices LIMIT 5"))
except:
    print("Invoices table not available or empty")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return final status
dbutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
