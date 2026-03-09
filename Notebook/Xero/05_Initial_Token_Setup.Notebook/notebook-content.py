# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "eed8c718-19d7-b81f-4045-1725ef15d326",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Initial Token Setup for Xero
# # MAGIC
# **Run this notebook ONCE to set up initial OAuth2 tokens.**
# # MAGIC
# This notebook is for first-time setup only. After running this once, the other notebooks will use the saved tokens and auto-refresh them.
# # MAGIC
# ## Instructions:
# 1. Set your Xero Client ID and Client Secret below
# 2. Run all cells
# 3. A browser will open for Xero login
# 4. Log in and authorize the app
# 5. Copy the authorization code from the redirect URL
# 6. Paste it in the widget when prompted
# 7. Tokens will be saved to lakehouse

# MARKDOWN ********************

# ## 1. Install Dependencies

# MARKDOWN ********************

# ## 2. Set Parameters

# CELL ********************

# Get parameter values
lakehouse_name = 'lh_bnj_bronze'
bronze_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/Xero/Data'
# silver_path = dbutils.widgets.get("silver_path")
client_id = 'D8B2CB74816B4BBE950CE686307D1C5C'
client_secret = 'LLyzptHeAtT1qEUHEEryFGhSN9mKlw8Z1emT4Yq1p17Y10Kx'
redirect_uri = 'http://localhost:3000'
token_file_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/xero/Token/xero_tokens.json'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Generate Authorization URL

# CELL ********************

from urllib.parse import urlencode

# Define scopes
# scopes = 'offline_access accounting.transactions accounting.settings accounting.contacts accounting.attachments'
scopes = "openid profile email accounting.transactions accounting.transactions.read accounting.settings accounting.settings.read accounting.contacts accounting.contacts.read accounting.reports.read offline_access"

# Generate authorization URL
auth_params = {
    'response_type': 'code',
    'client_id': client_id,
    'redirect_uri': redirect_uri,
    'scope': scopes,
    'state': 'fabric_setup'
}

auth_url = f"https://login.xero.com/identity/connect/authorize?{urlencode(auth_params)}"

print("\n" + "="*70)
print("STEP 1: AUTHORIZE IN BROWSER")
print("="*70)
print("\nPlease visit this URL in your browser:")
print("\n" + auth_url + "\n")
print("After authorizing, you'll be redirected to:")
print(f"{redirect_uri}?code=XXXXX&scope=...")
print("\nCopy the 'code' parameter from the URL")
print("="*70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Exchange Code for Tokens

# CELL ********************

import requests
import json
from datetime import datetime

authorization_code = 'D6XnpFaiNqULd_Ey5pRTbs2kae40gbTrg_xkrP9-Ijg'

if not authorization_code:
    raise ValueError("Please enter the authorization code!")

# Exchange code for tokens
token_url = 'https://identity.xero.com/connect/token'
token_data = {
    'grant_type': 'authorization_code',
    'client_id': client_id,
    'client_secret': client_secret,
    'code': authorization_code,
    'redirect_uri': redirect_uri
}

print("Exchanging authorization code for tokens...")

response = requests.post(token_url, data=token_data)

if response.status_code != 200:
    print(f"Error: {response.status_code}")
    print(response.text)
    raise Exception("Failed to get tokens")

token_json = response.json()

print("✓ Tokens received successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Save Tokens to Lakehouse

# CELL ********************

# Add metadata
token_json['saved_at'] = datetime.utcnow().isoformat()
token_json['setup_type'] = 'initial_browser_auth'

# Convert to JSON string
token_string = json.dumps(token_json, indent=2)

# Save to lakehouse
mssparkutils.fs.put(token_file_path, token_string, overwrite=True)

print(f"\n✓ Tokens saved to: {token_file_path}")
print("\nToken details:")
print(f"  - Access token expires in: {token_json['expires_in']} seconds (~30 minutes)")
print(f"  - Refresh token valid for: 60 days")
print(f"  - Scopes granted: {token_json['scope']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Verify Token

# CELL ********************

# Test the token by connecting to Xero
from xero_python.api_client import ApiClient
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.identity import IdentityApi

print("Verifying token by connecting to Xero...")

# Create OAuth2Token
oauth2_token = OAuth2Token(
    client_id=client_id,
    client_secret=client_secret
)

oauth2_token.update_token(
    access_token=token_json.get('access_token'),
    scope=token_json.get('scope', ''),
    expires_in=token_json.get('expires_in'),
    token_type=token_json.get('token_type', 'Bearer'),
    refresh_token=token_json.get('refresh_token'),
    id_token=token_json.get('id_token')
)

# Create API client
api_client = ApiClient(
    Configuration(oauth2_token=oauth2_token),
    pool_threads=1
)

# Test connection
identity_api = IdentityApi(api_client)
connections = identity_api.get_connections()

if connections:
    print("\n✓ Successfully connected to Xero!")
    print(f"\nConnected Organizations:")
    for conn in connections:
        print(f"  - {conn.tenant_name} (ID: {conn.tenant_id})")
else:
    print("\n⚠ Warning: No organizations found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Setup Complete!

# CELL ********************

print("\n" + "="*70)
print("SETUP COMPLETE!")
print("="*70)
print("\n✓ Token file created and saved to lakehouse")
print("✓ Connection to Xero verified")
print("✓ You can now run the main notebooks")
print("\nNext steps:")
print("1. Run 00_Orchestrator.py to extract all data")
print("2. Or run 04_Incremental_Load.py for incremental updates")
print("\nNote: You won't need to authorize again unless:")
print("  - The refresh token expires (after 60 days of inactivity)")
print("  - You delete the token file")
print("  - You change Xero organizations")
print("="*70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return success
dbutils.notebook.exit(json.dumps({
    'status': 'success',
    'token_file': token_file_path,
    'organizations': len(connections) if connections else 0
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
