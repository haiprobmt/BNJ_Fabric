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

# # Xero Data Extraction to Bronze Layer
# # MAGIC
# This notebook extracts data from Xero API and saves each data type as separate Delta tables in the bronze layer.
# # MAGIC
# **Output:** Separate Delta tables for each Xero data type

# MARKDOWN ********************

# ## 1. Setup and Configuration

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
from pyspark.sql.types import *
from datetime import datetime
import json

# Get parameters
bronze_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/Xero/Data'
extraction_timestamp = datetime.utcnow().isoformat()
extraction_date = datetime.utcnow().strftime("%Y-%m-%d")

print(f"Bronze Path: {bronze_path}")
print(f"Extraction Timestamp: {extraction_timestamp}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Functions

# CELL ********************

def flatten_complex_objects(data_dict, max_depth=2):
    """
    Flatten complex nested objects to avoid RecursionError in Spark schema inference
    Based on Xero data analysis: handles circular references (payment→invoice→payments)
    and deeply nested structures (contact with multiple sub-arrays)
    
    Args:
        data_dict: Dictionary to flatten
        max_depth: Maximum nesting depth before converting to JSON string (default: 2)
    
    Returns:
        Flattened dictionary with complex structures as JSON strings
    """
    def flatten_value(value, current_depth=0, parent_key=''):
        """Recursively flatten or convert to JSON string"""
        if value is None:
            return None
        
        # Convert to JSON string if max depth reached
        if current_depth >= max_depth:
            try:
                return json.dumps(value, default=str)
            except Exception:
                return str(value)
        
        # Handle lists - ALWAYS convert lists containing objects to JSON
        if isinstance(value, list):
            # Empty list is fine
            if not value:
                return []
            
            # Check first element - if it's complex, convert entire list to JSON
            first = value[0] if value else None
            if isinstance(first, (dict, list)):
                try:
                    return json.dumps(value, default=str)
                except Exception:
                    return str(value)
            
            # List of primitives only - keep as list
            return value
        
        # Handle dictionaries
        if isinstance(value, dict):
            # At depth 1, keep as dict but flatten nested objects
            if current_depth < max_depth:
                try:
                    flattened = {}
                    for k, v in value.items():
                        flattened[k] = flatten_value(v, current_depth + 1, k)
                    return flattened
                except RecursionError:
                    return json.dumps(value, default=str)
            else:
                # Beyond max depth, convert to JSON
                try:
                    return json.dumps(value, default=str)
                except Exception:
                    return str(value)
        
        # Return primitive types as-is (str, int, float, bool, datetime, etc.)
        return value
    
    # Process the dictionary
    try:
        result = {}
        for key, value in data_dict.items():
            try:
                result[key] = flatten_value(value, 0, key)
            except RecursionError:
                # If individual field fails, convert it to JSON
                print(f"  WARNING: Recursion detected in field '{key}', converting to JSON")
                try:
                    result[key] = json.dumps(value, default=str)
                except:
                    result[key] = str(value)
        return result
    except RecursionError:
        # If entire record fails, convert to JSON (shouldn't happen)
        print(f"  WARNING: Extreme recursion detected, converting entire record to JSON")
        return {"_raw_json": json.dumps(data_dict, default=str)}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Extraction Functions

# CELL ********************

def extract_xero_data(data_type, extract_func):
    """
    Extract data from Xero and return as list of dictionaries

    Args:
        data_type: Name of data type (e.g., 'invoices')
        extract_func: Function to call for extraction

    Returns:
        List of dictionaries containing the extracted data
    """
    try:
        print(f"\nExtracting {data_type}...")

        # Call extraction function
        result = extract_func()

        # Get items from result
        items = getattr(result, data_type, [])
        count = len(items)

        print(f"  Retrieved {count} {data_type}")

        # Convert to dictionaries
        data_list = []
        for item in items:
            if hasattr(item, 'to_dict'):
                data_dict = item.to_dict()
                # Add metadata
                data_dict['_extracted_at'] = extraction_timestamp
                data_dict['_extraction_date'] = extraction_date
                
                # Flatten complex nested objects to avoid RecursionError
                data_dict = flatten_complex_objects(data_dict)
                
                data_list.append(data_dict)

        return data_list, None

    except Exception as e:
        error_msg = f"Error extracting {data_type}: {str(e)}"
        print(f"  ERROR: {error_msg}")
        return [], error_msg

def save_to_bronze_delta(data_list, data_type, bronze_path):
    """
    Save data to Delta table in bronze layer

    Args:
        data_list: List of dictionaries
        data_type: Name of data type
        bronze_path: Base path for bronze layer
    """
    if not data_list:
        print(f"  No data to save for {data_type}")
        return

    # Convert to Spark DataFrame
    df = spark.createDataFrame(data_list)

    # Add bronze layer metadata
    df = df.withColumn("_bronze_ingestion_time", current_timestamp()) \
           .withColumn("_source_system", lit("Xero API")) \
           .withColumn("_data_type", lit(data_type))

    # Define table path
    table_path = f"{bronze_path}/{data_type}"

    # Write to Delta table (append mode for incremental loads)
    df.write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .save(table_path)

    print(f"  Saved {len(data_list)} records to {table_path}")

    # Register as table
    spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_xero_{data_type} USING DELTA LOCATION '{table_path}'")

    return df

def process_extraction(data_type, extract_func, bronze_path):
    """
    Process a single extraction: extract data, save to bronze, and return results
    
    Args:
        data_type: Name of data type (e.g., 'invoices')
        extract_func: Function to call for extraction
        bronze_path: Base path for bronze layer
        
    Returns:
        Tuple of (result_dict, error_dict or None)
    """
    print(f"\n{'='*70}")
    print(f"{data_type.upper()}")
    print('='*70)
    
    # Extract data
    data_list, error = extract_xero_data(data_type, extract_func)
    
    if error:
        return None, {'data_type': data_type, 'error': error}
    
    # Save to bronze
    if data_list:
        df = save_to_bronze_delta(data_list, data_type, bronze_path)
        result = {
            'data_type': data_type,
            'record_count': len(data_list),
            'table_name': f'bronze_xero_{data_type}'
        }
    else:
        result = {
            'data_type': data_type,
            'record_count': 0,
            'table_name': f'bronze_xero_{data_type}'
        }
    
    return result, None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Extract All Data Types

# CELL ********************

# Define extraction mappings
extractions = [
    ('invoices', lambda: accounting_api.get_invoices(xero_tenant_id=tenant_id)),
    ('accounts', lambda: accounting_api.get_accounts(xero_tenant_id=tenant_id)),
    ('payments', lambda: accounting_api.get_payments(xero_tenant_id=tenant_id)),
    ('bank_transactions', lambda: accounting_api.get_bank_transactions(xero_tenant_id=tenant_id)),
    ('contacts', lambda: accounting_api.get_contacts(xero_tenant_id=tenant_id)),
    ('credit_notes', lambda: accounting_api.get_credit_notes(xero_tenant_id=tenant_id)),
    ('items', lambda: accounting_api.get_items(xero_tenant_id=tenant_id)),
    ('tax_rates', lambda: accounting_api.get_tax_rates(xero_tenant_id=tenant_id)),
    ('tracking_categories', lambda: accounting_api.get_tracking_categories(xero_tenant_id=tenant_id)),
    ('employees', lambda: accounting_api.get_employees(xero_tenant_id=tenant_id)),
    ('purchase_orders', lambda: accounting_api.get_purchase_orders(xero_tenant_id=tenant_id)),
    ('quotes', lambda: accounting_api.get_quotes(xero_tenant_id=tenant_id)),
    ('manual_journals', lambda: accounting_api.get_manual_journals(xero_tenant_id=tenant_id)),
    ('receipts', lambda: accounting_api.get_receipts(xero_tenant_id=tenant_id)),
    ('organisations', lambda: accounting_api.get_organisations(xero_tenant_id=tenant_id)),
    ('currencies', lambda: accounting_api.get_currencies(xero_tenant_id=tenant_id)),
    ('users', lambda: accounting_api.get_users(xero_tenant_id=tenant_id)),
    ('branding_themes', lambda: accounting_api.get_branding_themes(xero_tenant_id=tenant_id)),
]

# Track results
results = []
errors = []
total_records = 0

# Process each extraction using the unified function
for data_type, extract_func in extractions:
    result, error = process_extraction(data_type, extract_func, bronze_path)
    
    if error:
        errors.append(error)
    else:
        results.append(result)
        total_records += result['record_count']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Extraction Summary

# CELL ********************

print("\n" + "="*70)
print("EXTRACTION SUMMARY")
print("="*70)

for result in results:
    print(f"{result['data_type']:.<30} {result['record_count']:>10,} records")

print("-" * 70)
print(f"{'TOTAL RECORDS':.<30} {total_records:>10,}")

if errors:
    print(f"\nERRORS: {len(errors)}")
    for err in errors:
        print(f"  - {err['data_type']}: {err['error']}")

print("\nBronze tables created:")
for result in results:
    print(f"  - {result['table_name']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Create Extraction Metadata Table

# CELL ********************

# Create metadata DataFrame
metadata = {
    'extraction_id': str(datetime.utcnow().timestamp()),
    'extraction_timestamp': extraction_timestamp,
    'extraction_date': extraction_date,
    'total_records': total_records,
    'data_types_extracted': len(results),
    'errors_count': len(errors),
    'results': results,
    'errors': errors
}

metadata_df = spark.createDataFrame([metadata])

# Save metadata
metadata_path = f"{bronze_path}/_metadata/extractions"
metadata_df.write \
    .format("delta") \
    .mode("append") \
    .save(metadata_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_xero_extraction_metadata USING DELTA LOCATION '{metadata_path}'")

print(f"\nMetadata saved to {metadata_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return summary
exit_data = {
    'status': 'success',
    'total_records': total_records,
    'data_types': len(results),
    'errors': len(errors)
}

dbutils.notebook.exit(json.dumps(exit_data))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
