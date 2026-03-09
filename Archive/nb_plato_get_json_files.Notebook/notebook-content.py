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

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA if not exists md;
# MAGIC CREATE TABLE md.etl_ingestion (
# MAGIC     id INT,
# MAGIC     etl_group string,
# MAGIC     etl_job string,
# MAGIC     src_schema string,
# MAGIC     src_table string,
# MAGIC     tgt_schema string,
# MAGIC     tgt_table string,
# MAGIC     etl_logic string,
# MAGIC     created_on TIMESTAMP
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plato_domains = ['adjustment', 'appointment', 'contact', 'corporate', 'deliveryorder', 'expense', 'facility', 'inventory', 'invoice', 'letter', 
'patient', 'patient/note', 'payment', 'purchaseorder', 'purchaserequisition', 'supplier', 'task', 'transfer', 'systemsetup']

xero_domains = ['invoices', 'accounts', 'payments', 'bank_transactions', 'contacts', 'credit_notes', 'items', 'tax_rates', 'tracking_categories', 
'employees', 'purchase_orders', 'quotes', 'manual_journals', 'receipts', 'organisations', 'currencies', 'users', 'branding_themes']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Plato API
try:
    i = int(spark.sql('SELECT max(id) FROM md.etl_ingestion').collect()[0])
except:
    i = 0
for domain in plato_domains:
    i = i + 1
    if domain == 'patient/note':
        spark.sql(f"INSERT INTO md.etl_ingestion VALUES ({i}, 'Landing', '{domain}', 'plato', 'patient_note', 'plato', 'patient_note', '{domain}?start_date=2025-01-01&end_date=2025-01-30', CURRENT_TIMESTAMP)")
    else:
        spark.sql(f"INSERT INTO md.etl_ingestion VALUES ({i}, 'Landing', '{domain}', 'plato', '{domain}', 'plato', '{domain}', '{domain}?start_date=2025-01-01&end_date=2025-01-30', CURRENT_TIMESTAMP)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Xero API
try:
    i = int(spark.sql('SELECT max(id) FROM md.etl_ingestion').collect()[0])
except:
    i = 0
for domain in xero_domains:
    i = i + 1
    spark.sql(f"INSERT INTO md.etl_ingestion VALUES ({i}, 'Landing', '{domain}', 'xero', 'xero_{domain}', 'xero', '{domain}', null, CURRENT_TIMESTAMP)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from typing import Optional, Dict, Any

def get_plato_data(page: int = None,
                           base_url: str = "https://clinic.platomedical.com/api/drics/",
                           params_extra: Optional[Dict[str, str]] = None,
                           headers: Optional[Dict[str, str]] = None,
                           domain: str = "appointment",
                           timeout: int = 30) -> Optional[Dict[str, Any]]:
    """Fetch data from the Plato API between start_date and end_date.

    Args:
        start_date: YYYY-MM-DD start date.
        end_date: YYYY-MM-DD end date.
        base_url: Base endpoint (defaults to the provided Plato endpoint).
        params_extra: Extra query parameters to include.
        headers: Optional HTTP headers (e.g., for auth).
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response (dict) on success, or None on error.
    """
    params = {"current_page": page}
    if params_extra:
        params.update(params_extra)

    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)

    # Construct full URL
    full_url = f"{base_url}{domain}"

    try:
        resp = requests.get(full_url, params=params, headers=req_headers, timeout=timeout)
        resp.raise_for_status()

        # Try to parse JSON
        data = resp.json()

        # Basic pagination handling (if API returns a 'next' URL or similar)
        # If the API provides a 'next' link in the JSON, fetch all pages.
        # Adjust this to match the exact API pagination format if needed.
        if isinstance(data, dict) and data.get('next'):
            all_items = []
            # If the initial response contains items, try to merge
            # This assumes responses have a top-level list under a known key.
            # If the API returns a list directly, handle that too.
            if isinstance(data.get('results'), list):
                all_items.extend(data.get('results'))
            elif isinstance(data.get('data'), list):
                all_items.extend(data.get('data'))

            next_url = data.get('next')
            while next_url:
                nxt = requests.get(next_url, headers=req_headers, timeout=timeout)
                nxt.raise_for_status()
                nxt_data = nxt.json()
                if isinstance(nxt_data.get('results'), list):
                    all_items.extend(nxt_data.get('results'))
                elif isinstance(nxt_data.get('data'), list):
                    all_items.extend(nxt_data.get('data'))
                else:
                    # If next page format differs, stop to avoid infinite loop
                    break
                next_url = nxt_data.get('next')

            # Return merged list under a consistent key
            return {"results": all_items}

        return data

    except requests.HTTPError as he:
        print(f"HTTP error while fetching Plato data: {he} - {resp.status_code if 'resp' in locals() else 'N/A'}")
        try:
            print("Response body:", resp.text)
        except Exception:
            pass
        return None
    except requests.RequestException as re:
        print(f"Request error while fetching Plato data: {re}")
        return None
    except ValueError as ve:
        print(f"Failed to parse JSON response: {ve}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DELETE FROM lh_bnj_bronze.md.etl_ingestion where tgt_schema = 'plato'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_bnj_bronze.md.etl_ingestion order by tgt_schema LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_pages(domain):
    # Run example: fetch data between 2025-01-01 and 2025-01-30
    headers = {"Authorization": "Bearer 4573dfbca452d15dbe808c1eb9982798"}
    # result = get_plato_data("2025-01-01", "2025-01-30", headers=headers, domain="appointment")
    result = get_plato_data(headers=headers, domain=f"{domain}/count")
    page_number = result['pages']
    batch_size = result['per_page']
    return page_number

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

page_number = get_pages(domain)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run example: fetch data between 2025-01-01 and 2025-01-30
import time
import json
headers = {"Authorization": "Bearer 4573dfbca452d15dbe808c1eb9982798"}
for domain in domains:
    print(f"Fetching {domain} table data")
    data = []
    for page in range(1, page_number + 1):
        try:
            result = get_plato_data(page=page, headers=headers, domain=domain)
            data.extend(result)
        except:
            print(f"Rate limit exceeded at page {page}, sleeping for 30 seconds...")
            timesleep = time.sleep(30)
    data = json.dumps(data)
    mssparkutils.fs.put(
        f"abfss://b523d5d1-6778-4d25-b980-d4f668a6c977@onelake.dfs.fabric.microsoft.com/7d7405e5-aa8c-4984-845f-f2234dcda2c4/Files/{domain}.json",
        data,
        overwrite=True
    )
    print(f"Total records fetched: {len(data)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
