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

from datetime import datetime
import json
import os

# Create extraction folder with timestamp
extraction_timestamp = datetime.utcnow()
extraction_id = batch_id
extraction_folder = f"{lakehouse_path}/{extraction_id}"

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
    import json
    
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


def extract_and_save_data_type(data_type, extract_func, extraction_folder):
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
    
    try:
        print(f"\nExtracting {data_type}...")
        
        # Call extraction function
        result = extract_func()
        
        # Get items from result
        items = getattr(result, data_type, [])
        count = len(items)
        
        print(f"  Retrieved {count} {data_type}")
        
        # Serialize to dictionaries
        serialized_items = serialize_xero_object(items)
        
        # Prepare data for saving
        output_data = {
            'extracted_at': extraction_timestamp.isoformat(),
            'extraction_id': extraction_id,
            'tenant_id': tenant_id,
            'data_type': data_type,
            'record_count': count,
            'records': serialized_items
        }
        
        # Save to file
        filename = f"xero_{data_type}.json"
        file_path = save_json_to_lakehouse(output_data, filename, extraction_folder)
        
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
            'data_type': data_type,
            'record_count': count,
            'filename': filename,
            'file_path': file_path,
            'file_size_mb': round(file_size_mb, 2),
            'status': 'success',
            'error': None
        }
        
    except Exception as e:
        error_msg = f"Error extracting {data_type}: {str(e)}"
        print(f"  ERROR: {error_msg}")
        
        return {
            'data_type': data_type,
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

# MARKDOWN ********************

# ## 3. Extract Core Accounting Data

# CELL ********************

print("="*70)
print("EXTRACTING XERO DATA")
print("="*70)

# Define all data types to extract
extractions = [
    # ('reports', lambda: accounting_api.get_reports(xero_tenant_id=tenant_id))
    ('invoices', lambda: accounting_api.get_invoices(xero_tenant_id=tenant_id)),
    ('accounts', lambda: accounting_api.get_accounts(xero_tenant_id=tenant_id)),
    ('payments', lambda: accounting_api.get_payments(xero_tenant_id=tenant_id)),
    ('bank_transactions', lambda: accounting_api.get_bank_transactions(xero_tenant_id=tenant_id)),
    ('contacts', lambda: accounting_api.get_contacts(xero_tenant_id=tenant_id)),
    # ('credit_notes', lambda: accounting_api.get_credit_notes(xero_tenant_id=tenant_id)),
    # ('items', lambda: accounting_api.get_items(xero_tenant_id=tenant_id)),
    # ('tax_rates', lambda: accounting_api.get_tax_rates(xero_tenant_id=tenant_id)),
    # ('tracking_categories', lambda: accounting_api.get_tracking_categories(xero_tenant_id=tenant_id)),
    # ('employees', lambda: accounting_api.get_employees(xero_tenant_id=tenant_id)),
    # ('purchase_orders', lambda: accounting_api.get_purchase_orders(xero_tenant_id=tenant_id)),
    # ('quotes', lambda: accounting_api.get_quotes(xero_tenant_id=tenant_id)),
    # ('manual_journals', lambda: accounting_api.get_manual_journals(xero_tenant_id=tenant_id)),
    # ('receipts', lambda: accounting_api.get_receipts(xero_tenant_id=tenant_id)),
    # ('organisations', lambda: accounting_api.get_organisations(xero_tenant_id=tenant_id)),
    # ('currencies', lambda: accounting_api.get_currencies(xero_tenant_id=tenant_id)),
    # ('users', lambda: accounting_api.get_users(xero_tenant_id=tenant_id)),
    # ('branding_themes', lambda: accounting_api.get_branding_themes(xero_tenant_id=tenant_id)),
]

# Track results
results = []
total_records = 0
total_size_mb = 0

# Extract and save each data type
for data_type, extract_func in extractions:
    #extraction_folder = lakehouse_path
    result = extract_and_save_data_type(data_type, extract_func, extraction_folder)
    print(result)
    results.append(result)
    
    if result['status'] == 'success':
        total_records += result['record_count']
        total_size_mb += result['file_size_mb']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Extract Reports (Optional)

# CELL ********************

import requests
from typing import Optional, Dict, Any

def get_profit_and_loss(
    access_token: str,
    tenant_id: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    periods: Optional[int] = None,
    timeframe: Optional[str] = None,
    tracking_category_id: Optional[str] = None,
    tracking_option_id: Optional[str] = None,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Retrieve Profit and Loss report from Xero.

    Parameters
    ----------
    access_token : str
        OAuth2 Bearer token
    tenant_id : str
        Xero tenant ID
    from_date : str, optional
        Start date (YYYY-MM-DD)
    to_date : str, optional
        End date (YYYY-MM-DD)
    periods : int, optional
        Number of periods (e.g. 12 for last 12 months)
    timeframe : str, optional
        'MONTH', 'QUARTER', 'YEAR'
    tracking_category_id : str, optional
        Tracking category ID
    tracking_option_id : str, optional
        Tracking option ID
    timeout : int
        HTTP timeout in seconds

    Returns
    -------
    dict
        Raw JSON response from Xero
    """

    url = "https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }

    params = {}

    if from_date:
        params["fromDate"] = from_date
    if to_date:
        params["toDate"] = to_date
    if periods:
        params["periods"] = periods
    if timeframe:
        params["timeframe"] = timeframe
    if tracking_category_id:
        params["trackingCategoryID"] = tracking_category_id
    if tracking_option_id:
        params["trackingOptionID"] = tracking_option_id

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=timeout
    )

    response.raise_for_status()
    return response.json()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

def xero_profit_and_loss_to_df(xero_response: dict) -> pd.DataFrame:
    report = xero_response["Reports"][0]
    rows = report["Rows"]

    # Extract period from header
    header = next(r for r in rows if r["RowType"] == "Header")
    period_label = header["Cells"][1]["Value"]

    data = []

    for row in rows:
        if row["RowType"] != "Section":
            continue

        section_name = row.get("Title", "").strip() or "Summary"

        for section_row in row.get("Rows", []):
            if section_row["RowType"] != "Row":
                continue

            cells = section_row["Cells"]

            account_name = cells[0]["Value"]
            amount = cells[1]["Value"]

            # Extract account ID if exists
            account_id = None
            attrs = cells[0].get("Attributes", [])
            for attr in attrs:
                if attr["Id"] == "account":
                    account_id = attr["Value"]

            data.append({
                "section": section_name,
                "account_name": account_name,
                "account_id": account_id,
                "period": period_label,
                "amount": float(amount.replace(",", "")),
                "report_date": report["ReportDate"]
            })

    return pd.DataFrame(data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from typing import Optional, Dict, Any

def get_aged_receivables_by_contact(
    access_token: str,
    tenant_id: str,
    contact_id: str,
    as_at_date: Optional[str] = None,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Query Xero Aged Receivables By Contact report.

    Parameters
    ----------
    access_token : str
        OAuth2 Bearer token from Xero
    tenant_id : str
        Xero tenant ID
    contact_id : str
        Xero ContactID (UUID)
    as_at_date : str, optional
        Report date in YYYY-MM-DD format.
        If None, Xero defaults to end of current month.
    timeout : int
        Request timeout in seconds

    Returns
    -------
    dict
        JSON response from Xero API
    """

    url = "https://api.xero.com/api.xro/2.0/Reports/AgedReceivablesByContact"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }

    params = {
        "contactID": contact_id
    }

    # Optional but highly recommended
    if as_at_date:
        params["date"] = as_at_date

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=timeout
    )

    response.raise_for_status()
    return response.json()

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
        'total_size_mb': round(total_size_mb, 2),
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
        print(f"{status_icon} {result['data_type']:.<30} {result['record_count']:>10,} records  {result['file_size_mb']:>8.2f} MB")
    else:
        print(f"{status_icon} {result['data_type']:.<30} ERROR")

print("-" * 70)
print(f"{'TOTAL':.<30} {total_records:>10,} records  {total_size_mb:>8.2f} MB")

# Show errors if any
errors = [r for r in results if r['status'] == 'error']
if errors:
    print(f"\nERRORS ({len(errors)}):")
    for err in errors:
        print(f"  - {err['data_type']}: {err['error']}")

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

# Create an index of all extractions
index_file = f"{lakehouse_path}/extraction_index.json"

# Load existing index or create new
try:
    with open(index_file, 'r') as f:
        index = json.load(f)
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
    'total_size_mb': round(total_size_mb, 2),
    'data_types': len(results),
    'status': 'success' if len(errors) == 0 else 'completed_with_errors'
})

# Save index
with open(index_file, 'w') as f:
    json.dump(index, f, indent=2)

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
    'total_size_mb': round(total_size_mb, 2),
    'data_types': len(results),
    'errors': len(errors)
}

mssparkutils.notebook.exit(json.dumps(exit_data))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
