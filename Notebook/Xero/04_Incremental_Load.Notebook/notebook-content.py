# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Incremental Load from Xero
# # MAGIC
# This notebook performs incremental loads from Xero based on last modified date.
# Only extracts records that have been modified since the last extraction.

# MARKDOWN ********************

# ## 1. Setup

# CELL ********************

%run ./02_Xero_Authentication

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import json

# Parameters
bronze_path = dbutils.widgets.get("bronze_path")
dbutils.widgets.text("lookback_days", "1", "Lookback Days")
lookback_days = int(dbutils.widgets.get("lookback_days"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Get Last Extraction Time

# CELL ********************

def get_last_extraction_time(data_type):
    """Get the last extraction time for a data type"""
    try:
        # Query the bronze table for last extraction timestamp
        df = spark.sql(f"""
            SELECT MAX(_extracted_at) as last_extracted
            FROM bronze_xero_{data_type}
        """)

        last_extracted = df.collect()[0]['last_extracted']

        if last_extracted:
            last_time = datetime.fromisoformat(last_extracted)
            print(f"{data_type}: Last extracted at {last_time}")
            return last_time
        else:
            # No previous extraction, use lookback days
            last_time = datetime.utcnow() - timedelta(days=lookback_days)
            print(f"{data_type}: No previous extraction, using {lookback_days} days lookback")
            return last_time

    except Exception as e:
        # Table doesn't exist, use lookback days
        last_time = datetime.utcnow() - timedelta(days=lookback_days)
        print(f"{data_type}: Table not found, using {lookback_days} days lookback")
        return last_time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Incremental Extraction

# CELL ********************

# Data types that support if_modified_since parameter
incremental_data_types = [
    'invoices',
    'contacts',
    'payments',
    'bank_transactions',
    'credit_notes',
    'purchase_orders',
    'manual_journals',
    'employees'
]

extraction_timestamp = datetime.utcnow().isoformat()
results = []
total_new_records = 0

for data_type in incremental_data_types:
    print(f"\n{'='*70}")
    print(f"Incremental Load: {data_type.upper()}")
    print('='*70)

    try:
        # Get last extraction time
        last_extracted = get_last_extraction_time(data_type)

        # Call Xero API with if_modified_since
        extract_func = getattr(accounting_api, f'get_{data_type}')
        result = extract_func(
            xero_tenant_id=tenant_id,
            if_modified_since=last_extracted
        )

        # Get items
        items = getattr(result, data_type, [])
        count = len(items)

        print(f"  Found {count} modified records")

        if count > 0:
            # Convert to dictionaries
            data_list = []
            for item in items:
                if hasattr(item, 'to_dict'):
                    data_dict = item.to_dict()
                    data_dict['_extracted_at'] = extraction_timestamp
                    data_dict['_extraction_date'] = datetime.utcnow().strftime("%Y-%m-%d")
                    data_list.append(data_dict)

            # Save to bronze (append mode)
            df = spark.createDataFrame(data_list)
            df = df.withColumn("_bronze_ingestion_time", current_timestamp()) \
                   .withColumn("_source_system", lit("Xero API")) \
                   .withColumn("_data_type", lit(data_type)) \
                   .withColumn("_incremental_load", lit(True))

            table_path = f"{bronze_path}/{data_type}"
            df.write \
              .format("delta") \
              .mode("append") \
              .option("mergeSchema", "true") \
              .save(table_path)

            print(f"  Appended {count} records to bronze")

            total_new_records += count
            results.append({
                'data_type': data_type,
                'new_records': count,
                'last_extracted': last_extracted.isoformat()
            })

    except Exception as e:
        print(f"  ERROR: {str(e)}")
        results.append({
            'data_type': data_type,
            'error': str(e)
        })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Summary

# CELL ********************

print("\n" + "="*70)
print("INCREMENTAL LOAD SUMMARY")
print("="*70)

for result in results:
    if 'error' in result:
        print(f"{result['data_type']:.<30} ERROR: {result['error']}")
    else:
        print(f"{result['data_type']:.<30} {result['new_records']:>10} new records")

print("-" * 70)
print(f"{'TOTAL NEW RECORDS':.<30} {total_new_records:>10}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dbutils.notebook.exit(json.dumps({
    'total_new_records': total_new_records,
    'data_types': len(results)
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
