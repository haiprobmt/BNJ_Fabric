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
# META     },
# META     "environment": {
# META       "environmentId": "eed8c718-19d7-b81f-4045-1725ef15d326",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Xero Data Extractor to Fabric Lakehouse
# # MAGIC
# This notebook extracts data from Xero API and saves each data type as separate JSON files in the Fabric lakehouse.
# # MAGIC
# **Features:**
# - Extracts all Xero data types
# - Splits data into separate files by type
# - Saves to lakehouse Files section
# - Creates extraction metadata
# # MAGIC
# **Output Structure:**
# ```
# Files/
#   xero/
#     YYYYMMDDHHMMSS/
#       xero_invoices.json
#       xero_accounts.json
#       xero_payments.json
#       ...
#       extraction_metadata.json
# ```

# PARAMETERS CELL ********************

batch_id = 20260210101300
job_id = None
batch_group = "xero"
lakehouse_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/xero/Data'
include_reports = "true"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Setup and Configuration

# CELL ********************

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ./02_Xero_Authentication

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
import json
import time
# Create extraction folder with timestamp
extraction_timestamp = datetime.utcnow()
extraction_id = batch_id
extraction_folder = f"{lakehouse_path}/{extraction_id}"
start_time = time.time()

print(f"Lakehouse Path: {lakehouse_path}")
print(f"Extraction ID: {extraction_id}")
print(f"Extraction Folder: {extraction_folder}")
print(f"Include Reports: {include_reports}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Helper Functions

# CELL ********************

def serialize_xero_object(obj):
    """
    Convert Xero API objects to JSON-serializable dictionaries
    
    Args:
        obj: Xero object or any Python object
        
    Returns:
        JSON-serializable version of the object
    """
    if obj is None:
        return None
    elif hasattr(obj, 'to_dict'):
        return obj.to_dict()
    elif isinstance(obj, list):
        return [serialize_xero_object(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: serialize_xero_object(v) for k, v in obj.items()}
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj


def save_json_to_lakehouse(data, filename, folder_path):
    """
    Save data as JSON file to lakehouse using mssparkutils
    
    Args:
        data: Data to save
        filename: Name of the file
        folder_path: Folder path in lakehouse (ABFSS path)
        
    Returns:
        Full path of saved file
    """
    from notebookutils import mssparkutils
    
    # Create full file path
    file_path = f"{folder_path}/{filename}"
    
    # Convert data to JSON string
    json_str = json.dumps(data, indent=2, default=str)
    
    # Ensure folder exists
    try:
        mssparkutils.fs.mkdirs(folder_path)
    except:
        pass  # Folder might already exist
    
    # Write file to lakehouse using mssparkutils
    mssparkutils.fs.put(file_path, json_str, overwrite=True)
    
    print(f"✓ Saved: {filename}")
    return file_path


def extract_and_save_data_type(p_job_id, domain, extract_func, extraction_folder):
    """
    Extract a single data type and save to JSON file
    
    Args:
        data_type: Name of data type (e.g., 'invoices')
        extract_func: Function to call for extraction
        extraction_folder: Folder to save the file
        
    Returns:
        Dictionary with extraction results
    """
    from notebookutils import mssparkutils

    print("\n===START JOB===")
    #start_job_instance(batch_id, p_job_id, msg=f"Start domain={domain}")

    try:
        print(f"Extracting {domain}...")
        
        # Call extraction function
        result = extract_func()
        
        # Get items from result
        items = getattr(result, domain, [])
        total_pages = len(items)
        
        print(f"  Retrieved {domain}")
        
        # Serialize to dictionaries
        serialized_items = serialize_xero_object(items)
        
        # Prepare data for saving
        output_data = {
            'extracted_at': extraction_timestamp.isoformat(),
            'extraction_id': extraction_id,
            'tenant_id': tenant_id,
            'data_type': domain,
            'record_count': total_pages,
            'records': serialized_items
        }
        

        # Save to file
        filename = f"xero_{domain}.json"
        out_path = save_json_to_lakehouse(output_data, filename, extraction_folder)

        # LOG: step success
        print("===END JOB SUCCESS===")
        end_job_instance(
            batch_id, p_job_id, "SUCCESS",
            msg=f"Completed domain={domain}. pages={total_pages}. output={out_path}",
            src_row_num=total_pages,
            tgt_row_num=total_pages
        )

        # Get file size using mssparkutils
        try:
            file_info = mssparkutils.fs.head(file_path, 0)  # Get file info without reading content
            # Alternative: list the specific file
            file_list = mssparkutils.fs.ls(file_path)
            if file_list and len(file_list) > 0:
                file_size = file_list[0].size
            else:
                # Fallback: estimate from JSON string
                import json
                file_size = len(json.dumps(output_data, default=str))
        except:
            # Fallback: estimate from JSON string
            import json
            file_size = len(json.dumps(output_data, default=str))
        
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"  Saved to: {filename} ({file_size_mb:.2f} MB)")
        
        return {
            'domain': domain,
            'record_count': total_pages,
            'filename': filename,
            'file_path': out_path,
            'file_size_mb': float(f"{file_size_mb:.2f}"),
            'status': 'success',
            'error': None
        }
        
    except Exception as e:
        error_msg = f"Error extracting {domain}: {str(e)}"
        print("===END JOB FAILED===")

        # # LOG: step failed
        end_job_instance(batch_id, p_job_id, "FAILED",msg=f"Failed domain={domain}. {safe_exception_text(e)}")

        print(f"  ERROR: {error_msg}")
        
        return {
            'domain': domain,
            'record_count': 0,
            'filename': None,
            'file_path': None,
            'file_size_mb': 0,
            'status': 'error',
            'error': error_msg
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

job_id_param = job_id
def look_up_job_config():
    jobs_df = (
        spark.table("`WS-ETL-BNJ`.lh_bnj_metadata.md.etl_jobs")
        .filter((col("active_flg") == "Y") & (col("src_catalog") == "xero"))
        .filter(col("job_group_name") == "staging")
        .select("job_id", "job_name")
    )

    # Apply filter only if job_id_param provided
    if job_id_param is not None and str(job_id_param).strip() != "":
        job_id_list = [x.strip() for x in str(job_id_param).split(",") if x.strip() != ""]
        # Compare as string to be safe even if job_id column is INT
        jobs_df = jobs_df.filter(col("job_id").cast("string").isin(job_id_list))

    jobs = jobs_df.orderBy("job_id").collect()

    return jobs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def check_timeout():
    if time.time() - start_time > 20 * 60:
        raise TimeoutError("Notebook timeout")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Extract Core Accounting Data

# CELL ********************

print("="*70)
print("EXTRACTING XERO DATA")
print("="*70)

job_list = look_up_job_config()

API_REGISTRY = {
    "contactgroups": lambda: accounting_api.get_contact_groups(xero_tenant_id=tenant_id),
    "accounts": lambda: accounting_api.get_accounts(xero_tenant_id=tenant_id),
    "payments": lambda: accounting_api.get_payments(xero_tenant_id=tenant_id),
    "bank_transactions": lambda: accounting_api.get_bank_transactions(xero_tenant_id=tenant_id),
    "contacts": lambda: accounting_api.get_contacts(xero_tenant_id=tenant_id),
    "credit_notes": lambda: accounting_api.get_credit_notes(xero_tenant_id=tenant_id),
    "items": lambda: accounting_api.get_items(xero_tenant_id=tenant_id),
    "tax_rates": lambda: accounting_api.get_tax_rates(xero_tenant_id=tenant_id),
    "tracking_categories": lambda: accounting_api.get_tracking_categories(xero_tenant_id=tenant_id),
    "employees": lambda: accounting_api.get_employees(xero_tenant_id=tenant_id),
    "purchase_orders": lambda: accounting_api.get_purchase_orders(xero_tenant_id=tenant_id),
    "quotes": lambda: accounting_api.get_quotes(xero_tenant_id=tenant_id),
    "manual_journals": lambda: accounting_api.get_manual_journals(xero_tenant_id=tenant_id),
    "receipts": lambda: accounting_api.get_receipts(xero_tenant_id=tenant_id),
    "organisations": lambda: accounting_api.get_organisations(xero_tenant_id=tenant_id),
    "currencies": lambda: accounting_api.get_currencies(xero_tenant_id=tenant_id),
    "users": lambda: accounting_api.get_users(xero_tenant_id=tenant_id),
    "branding_themes": lambda: accounting_api.get_branding_themes(xero_tenant_id=tenant_id),
    "invoices": lambda: accounting_api.get_branding_themes(xero_tenant_id=tenant_id)
}


# # Track results
results = []
total_records = 0
total_size_mb = 0

for row in job_list:
    r_job_name = row['job_name']
    r_job_id = row['job_id']

    extract_func = API_REGISTRY.get(r_job_name)

    if extract_func is None:
        print(f"⚠️ Skipping job '{r_job_name}' – no API mapping")
        results.append({
            "job_id": r_job_id,
            "job_name": r_job_name,
            "status": "skipped",
            "reason": "API not implemented"
        })
        continue

    result = extract_and_save_data_type(r_job_id, r_job_name, extract_func, extraction_folder)
    results.append(result)
    
    if result['status'] == 'success':
        total_records += result['record_count']
        total_size_mb += result['file_size_mb']

    #Ensure timeout after more than 30 minutes run
    time.sleep(2)
    check_timeout()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Save Extraction Metadata

# CELL ********************

# Create metadata
metadata = {
    'extraction_id': extraction_id,
    'extracted_at': extraction_timestamp.isoformat(),
    'tenant_id': tenant_id,
    'lakehouse_path': lakehouse_path,
    'extraction_folder': extraction_folder,
    'include_reports': include_reports,
    'summary': {
        'total_data_types': len(results),
        'total_records': total_records,
        'total_size_mb': float(f"{total_size_mb:.2f}"),
        'successful': len([r for r in results if r['status'] == 'success']),
        'failed': len([r for r in results if r['status'] == 'error'])
    },
    'results': results
}

# Save metadata
metadata_path = save_json_to_lakehouse(
    metadata, 
    'extraction_metadata.json', 
    extraction_folder
)

print("\n" + "="*70)
print("EXTRACTION METADATA")
print("="*70)
print(f"Metadata saved to: extraction_metadata.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Extraction Summary

# CELL ********************

print("\n" + "="*70)
print("EXTRACTION SUMMARY")
print("="*70)
print(f"Extraction ID: {extraction_id}")
print(f"Extraction Folder: {extraction_folder}")
print(f"\nData Types Extracted:")
print("-" * 70)

for result in results:
    status_icon = "✓" if result['status'] == 'success' else "✗"
    if result['status'] == 'success':
        print(f"{status_icon} {result['domain']:.<30} {result['record_count']:>10,} records  {result['file_size_mb']:>8.2f} MB")
    else:
        print(f"{status_icon} {result['domain']:.<30} ERROR")

print("-" * 70)
print(f"{'TOTAL':.<30} {total_records:>10,} records  {total_size_mb:>8.2f} MB")

# Show errors if any
errors = [r for r in results if r['status'] == 'error']
if errors:
    print(f"\nERRORS ({len(errors)}):")
    for err in errors:
        print(f"  - {err['domain']}: {err['error']}")

print(f"\nAll files saved to:")
print(f"  {extraction_folder}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Create Index File

# CELL ********************

import json

# Create an index of all extractions
index_file = f"{extraction_folder}/extraction_index.json"

# Load existing index or create new
try:
    existing_content = notebookutils.fs.head(index_file)
    index = json.loads(existing_content)
except:
    index = {
        'extractions': []
    }

# Add this extraction
index['extractions'].append({
    'extraction_id': extraction_id,
    'extracted_at': extraction_timestamp.isoformat(),
    'folder': extraction_folder,
    'total_records': total_records,
    'total_size_mb': float(f"{total_size_mb:.2f}"),
    'data_types': len(results),
    'status': 'success' if len(errors) == 0 else 'completed_with_errors'
})

# Save index back to ADLS / OneLake
index_json = json.dumps(index, indent=2)
notebookutils.fs.put(index_file, index_json, overwrite=True)

print(f"\nExtraction index updated: {index_file}")
print(f"Total extractions in index: {len(index['extractions'])}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return summary for orchestration
exit_data = {
    'status': 'success' if len(errors) == 0 else 'completed_with_errors',
    'extraction_id': extraction_id,
    'extraction_folder': extraction_folder,
    'total_records': total_records,
    'total_size_mb':float(f"{total_size_mb:.2f}"),
    'data_types': len(results),
    'errors': len(errors)
}

mssparkutils.notebook.exit(json.dumps(exit_data))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import explode

df_raw = (
    spark.read
    .option("multiline", "true")
    .json("Files/xero/Data/20260130101804/xero_contactgroups.json")
)

df_invoices = (
    df_raw
    .select(explode("records").alias("contactgroups"))
    .select("contactgroups.*")
)

display(df_raw)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
