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

# # Xero Data Extraction - Configuration and Setup
# # MAGIC
# This notebook sets up the configuration and parameters for Xero data extraction to Fabric Lakehouse.
# # MAGIC
# **Parameters:**
# - `lakehouse_name`: Target lakehouse name
# - `bronze_path`: Path to bronze layer
# - `client_id`: Xero OAuth2 Client ID
# - `client_secret`: Xero OAuth2 Client Secret
# - `token_file_path`: Path to store OAuth tokens

# MARKDOWN ********************

# ## 1. Define Parameters

# CELL ********************

from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Get parameter values
lakehouse_name = 'lh_bnj_bronze'
bronze_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/Xero/Data'
# silver_path = dbutils.widgets.get("silver_path")
client_id = 'D8B2CB74816B4BBE950CE686307D1C5C'
client_secret = 'LLyzptHeAtT1qEUHEEryFGhSN9mKlw8Z1emT4Yq1p17Y10Kx'
redirect_uri = 'http://localhost:3000'
token_file_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/Xero/Token'
extraction_date = datetime.now()
full_extract = "true"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Install Required Packages

# CELL ********************

#%pip install xero-python requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Import Libraries

# CELL ********************

import json
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Create Directory Structure

# CELL ********************

# Create bronze layer directories for each data type
data_types = [
    'invoices',
    'accounts',
    'contacts',
    'payments',
    'bank_transactions',
    'expense_claims',
    'items',
    'tax_rates',
    'tracking_categories',
    'employees',
    'credit_notes',
    'purchase_orders',
    'quotes',
    'manual_journals',
    'receipts',
    'organisations',
    'currencies',
    'users'
]

for data_type in data_types:
    path = f"{bronze_path}/{data_type}"
    mssparkutils.fs.mkdirs(path)
    print(f"Created: {path}")

# Create config directory
mssparkutils.fs.mkdirs(os.path.dirname(token_file_path))
print(f"Created config directory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Configuration Summary

# CELL ********************

import json
from datetime import datetime, date

def json_safe(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value

config = {
    "lakehouse_name": lakehouse_name,
    "bronze_path": bronze_path,
    "client_id": client_id[:10] + "..." if client_id else "Not set",
    "redirect_uri": redirect_uri,
    "token_file_path": token_file_path,
    "extraction_date": json_safe(extraction_date),
    "full_extract": full_extract,
    "data_types": data_types
}

print("Configuration:")
print(json.dumps(config, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Store config in notebook scope for other notebooks
mssparkutils.notebook.exit(json.dumps(config))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
