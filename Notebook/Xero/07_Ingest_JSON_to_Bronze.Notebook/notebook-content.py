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

# # Ingest Split JSON Files to Bronze Delta Tables
# # MAGIC
# This notebook dynamically ingests split Xero JSON files into bronze Delta tables.
# # MAGIC
# **Features:**
# - Automatically discovers all JSON files in source folder
# - Flattens complex nested structures to avoid RecursionError
# - Creates separate Delta table for each data type
# - Handles schema evolution automatically
# - Tracks ingestion metadata
# # MAGIC
# **Input:** JSON files from split_data or lakehouse extraction folder
# **Output:** Bronze Delta tables in lakehouse

# MARKDOWN ********************

# ## 1. Setup and Configuration

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
import os
from pathlib import Path

source_path = "abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/Xero/Data"
bronze_path = "abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Tables"
overwrite = "true"

# Ingestion metadata
ingestion_timestamp = datetime.utcnow().isoformat()
ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")

print(f"Source Path: {source_path}")
print(f"Bronze Path: {bronze_path}")
print(f"Overwrite Mode: {overwrite}")
print(f"Ingestion Timestamp: {ingestion_timestamp}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Helper Functions

# CELL ********************

def flatten_complex_objects(data_dict, max_depth=2):
    """
    Flatten complex nested objects to avoid RecursionError in Spark schema inference
    Based on Xero data analysis: handles circular references and deeply nested structures
    
    Args:
        data_dict: Dictionary to flatten
        max_depth: Maximum nesting depth before converting to JSON string (default: 2)
    
    Returns:
        Flattened dictionary with complex structures as JSON strings
    """
    def flatten_value(value, current_depth=0):
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
                        flattened[k] = flatten_value(v, current_depth + 1)
                    return flattened
                except RecursionError:
                    return json.dumps(value, default=str)
            else:
                # Beyond max depth, convert to JSON
                try:
                    return json.dumps(value, default=str)
                except Exception:
                    return str(value)
        
        # Return primitive types as-is
        return value
    
    # Process the dictionary
    try:
        result = {}
        for key, value in data_dict.items():
            try:
                result[key] = flatten_value(value, 0)
            except RecursionError:
                print(f"  WARNING: Recursion detected in field '{key}', converting to JSON")
                try:
                    result[key] = json.dumps(value, default=str)
                except:
                    result[key] = str(value)
        return result
    except RecursionError:
        print(f"  WARNING: Extreme recursion detected, converting entire record to JSON")
        return {"_raw_json": json.dumps(data_dict, default=str)}


def discover_json_files(source_path):
    """
    Discover all JSON files in source path
    
    Args:
        source_path: Path to scan for JSON files
        
    Returns:
        List of tuples: (data_type, file_path)
    """
    json_files = []
    
    try:
        # List all files in directory
        files = os.listdir(source_path)
        
        for filename in files:
            # Skip non-JSON files and metadata files
            if not filename.endswith('.json'):
                continue
            if filename in ['extraction_metadata.json', 'extraction_summary.json', 'split_statistics.json']:
                continue
            
            # Extract data type from filename
            # Expected format: xero_<data_type>.json
            if filename.startswith('xero_'):
                data_type = filename.replace('xero_', '').replace('.json', '')
                file_path = os.path.join(source_path, filename)
                json_files.append((data_type, file_path))
        
        print(f"Discovered {len(json_files)} JSON files to process")
        return json_files
        
    except Exception as e:
        print(f"ERROR: Failed to discover files: {e}")
        return []


def read_and_flatten_json(file_path, data_type):
    """
    Read JSON file and flatten records
    
    Args:
        file_path: Path to JSON file
        data_type: Name of data type
        
    Returns:
        List of flattened dictionaries
    """
    try:
        print(f"\nReading: {os.path.basename(file_path)}")
        
        # Read JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract records (handle different JSON structures)
        if isinstance(data, dict):
            records = data.get('records', data.get('data', []))
        elif isinstance(data, list):
            records = data
        else:
            records = []
        
        record_count = len(records)
        print(f"  Found {record_count:,} records")
        
        if record_count == 0:
            return []
        
        # Flatten each record
        flattened_records = []
        for i, record in enumerate(records):
            if i % 1000 == 0 and i > 0:
                print(f"  Flattening... {i:,}/{record_count:,}")
            
            try:
                # Add metadata
                record['_ingested_at'] = ingestion_timestamp
                record['_ingestion_date'] = ingestion_date
                record['_source_file'] = os.path.basename(file_path)
                
                # Flatten complex structures
                flattened = flatten_complex_objects(record)
                flattened_records.append(flattened)
                
            except Exception as e:
                print(f"  WARNING: Failed to flatten record {i}: {e}")
                continue
        
        print(f"  Successfully flattened {len(flattened_records):,} records")
        return flattened_records
        
    except Exception as e:
        print(f"  ERROR: Failed to read file: {e}")
        return []


def save_to_bronze_delta(data_list, data_type, bronze_path, overwrite=False):
    """
    Save data to Delta table in bronze layer
    
    Args:
        data_list: List of dictionaries
        data_type: Name of data type
        bronze_path: Base path for bronze layer
        overwrite: Whether to overwrite existing table
        
    Returns:
        Dictionary with ingestion results
    """
    if not data_list:
        print(f"  No data to save for {data_type}")
        return {
            'data_type': data_type,
            'status': 'skipped',
            'record_count': 0,
            'error': 'No records to ingest'
        }
    
    try:
        # Convert to Spark DataFrame
        print(f"  Creating DataFrame...")
        df = spark.createDataFrame(data_list)
        
        # Add bronze layer metadata
        df = df.withColumn("_bronze_ingestion_time", current_timestamp()) \
               .withColumn("_source_system", lit("Xero API")) \
               .withColumn("_data_type", lit(data_type))
        
        # Define table path
        table_path = f"{bronze_path}/bronze_xero_{data_type}"
        table_name = f"bronze_xero_{data_type}"
        
        # Write mode
        write_mode = "overwrite" if overwrite else "append"
        
        # Write to Delta table
        print(f"  Writing to Delta table ({write_mode} mode)...")
        df.write \
          .format("delta") \
          .mode(write_mode) \
          .option("mergeSchema", "true") \
          .option("overwriteSchema", "true" if overwrite else "false") \
          .save(table_path)
        
        # Register as table
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{table_path}'")
        
        record_count = len(data_list)
        print(f"  ✓ Saved {record_count:,} records to {table_name}")
        
        return {
            'data_type': data_type,
            'status': 'success',
            'record_count': record_count,
            'table_name': table_name,
            'table_path': table_path,
            'write_mode': write_mode,
            'error': None
        }
        
    except Exception as e:
        error_msg = f"Error saving {data_type} to Delta: {str(e)}"
        print(f"  ✗ ERROR: {error_msg}")
        
        return {
            'data_type': data_type,
            'status': 'error',
            'record_count': 0,
            'table_name': None,
            'table_path': None,
            'write_mode': write_mode,
            'error': error_msg
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Discover and Process JSON Files

# CELL ********************

print("="*70)
print("DISCOVERING JSON FILES")
print("="*70)

# Discover all JSON files
json_files = discover_json_files(source_path)

if not json_files:
    print("\nNo JSON files found to process!")
    dbutils.notebook.exit(json.dumps({'status': 'error', 'message': 'No JSON files found'}))

print("\nFiles to process:")
for data_type, file_path in json_files:
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"  - {data_type:.<30} {file_size_mb:>8.2f} MB")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Ingest to Bronze Delta Tables

# CELL ********************

print("\n" + "="*70)
print("INGESTING TO BRONZE DELTA TABLES")
print("="*70)

# Track results
results = []
total_records = 0

# Process each JSON file
for data_type, file_path in json_files:
    print(f"\n{'='*70}")
    print(f"{data_type.upper()}")
    print('='*70)
    
    # Read and flatten records
    flattened_records = read_and_flatten_json(file_path, data_type)
    
    # Save to bronze Delta
    result = save_to_bronze_delta(flattened_records, data_type, bronze_path, overwrite)
    results.append(result)
    
    if result['status'] == 'success':
        total_records += result['record_count']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Ingestion Summary

# CELL ********************

print("\n" + "="*70)
print("INGESTION SUMMARY")
print("="*70)

successful = [r for r in results if r['status'] == 'success']
failed = [r for r in results if r['status'] == 'error']
skipped = [r for r in results if r['status'] == 'skipped']

print(f"\nResults:")
print(f"  Total files processed: {len(json_files)}")
print(f"  Successful: {len(successful)}")
print(f"  Failed: {len(failed)}")
print(f"  Skipped: {len(skipped)}")
print(f"  Total records ingested: {total_records:,}")

print("\nDetails:")
print("-" * 70)
for result in results:
    status_icon = "✓" if result['status'] == 'success' else ("✗" if result['status'] == 'error' else "⊘")
    print(f"{status_icon} {result['data_type']:.<30} {result['record_count']:>10,} records")

print("-" * 70)
print(f"{'TOTAL':.<30} {total_records:>10,} records")

# Show errors if any
if failed:
    print(f"\nERRORS ({len(failed)}):")
    for err in failed:
        print(f"  - {err['data_type']}: {err['error']}")

# Show created tables
if successful:
    print(f"\nBronze tables created/updated:")
    for result in successful:
        print(f"  - {result['table_name']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Save Ingestion Metadata

# CELL ********************

# Create ingestion metadata
ingestion_metadata = {
    'ingestion_timestamp': ingestion_timestamp,
    'ingestion_date': ingestion_date,
    'source_path': source_path,
    'bronze_path': bronze_path,
    'overwrite_mode': overwrite,
    'summary': {
        'total_files': len(json_files),
        'total_records': total_records,
        'successful': len(successful),
        'failed': len(failed),
        'skipped': len(skipped)
    },
    'results': results
}

# Save metadata to Delta table
metadata_path = f"{bronze_path}/_metadata/ingestion_history"
metadata_df = spark.createDataFrame([ingestion_metadata])

metadata_df.write \
    .format("delta") \
    .mode("append") \
    .save(metadata_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_ingestion_metadata USING DELTA LOCATION '{metadata_path}'")

print(f"\nIngestion metadata saved to: bronze_ingestion_metadata")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Table Statistics

# CELL ********************

print("\n" + "="*70)
print("TABLE STATISTICS")
print("="*70)

for result in successful:
    table_name = result['table_name']
    
    # Get row count
    row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]['cnt']
    
    # Get table details
    try:
        details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        size_mb = details['sizeInBytes'] / (1024 * 1024) if 'sizeInBytes' in details else 0
        num_files = details['numFiles'] if 'numFiles' in details else 0
        
        print(f"\n{table_name}:")
        print(f"  Rows: {row_count:,}")
        print(f"  Size: {size_mb:.2f} MB")
        print(f"  Files: {num_files}")
    except:
        print(f"\n{table_name}:")
        print(f"  Rows: {row_count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return summary for orchestration
exit_data = {
    'status': 'success' if len(failed) == 0 else 'completed_with_errors',
    'total_records': total_records,
    'tables_created': len(successful),
    'errors': len(failed)
}

dbutils.notebook.exit(json.dumps(exit_data))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
