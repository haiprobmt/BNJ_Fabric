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

# # Quick Fix: Add Tenant ID to Existing Token
# # MAGIC
# Run this notebook if you're getting "No Xero organizations found" error.
# This will manually add the tenant_id to your existing token.

# MARKDOWN ********************

# ## 1. Parameters

# CELL ********************

token_file_path = "abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/Xero/Token/xero_token.json"
# tenant_id_input = dbutils.widgets.get("tenant_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Existing Token

# CELL ********************

import json

# Load token
token_content = mssparkutils.fs.head(token_file_path, 10000)
token_dict = json.loads(token_content)

print("Current token info:")
print(f"  Scopes: {token_dict.get('scope', 'N/A')}")
print(f"  Has tenant_id: {'tenant_id' in token_dict}")
print(f"  Saved at: {token_dict.get('saved_at', 'N/A')}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Option A: Auto-Detect Tenant ID (Try This First)

# CELL ********************

# Try to get tenant_id from Xero API
try:
    # Import required modules
    from xero_python.api_client import ApiClient
    from xero_python.api_client.configuration import Configuration
    from xero_python.api_client.oauth2 import OAuth2Token
    from xero_python.identity import IdentityApi

    # Create OAuth2Token
    oauth2_token = OAuth2Token(
        client_id=token_dict.get('client_id', ''),
        client_secret=token_dict.get('client_secret', '')
    )

    oauth2_token.update_token(
        access_token=token_dict.get('access_token'),
        scope=token_dict.get('scope', ''),
        expires_in=token_dict.get('expires_in'),
        token_type=token_dict.get('token_type', 'Bearer'),
        refresh_token=token_dict.get('refresh_token'),
        id_token=token_dict.get('id_token')
    )

    # Create API client
    api_client = ApiClient(
        Configuration(oauth2_token=oauth2_token),
        pool_threads=1
    )

    # Try to get connections
    identity_api = IdentityApi(api_client)
    connections = identity_api.get_connections()

    if connections:
        print(f"\n✓ Found {len(connections)} organization(s):")
        for i, conn in enumerate(connections):
            print(f"\n{i+1}. {conn.tenant_name}")
            print(f"   Tenant ID: {conn.tenant_id}")
            print(f"   Type: {conn.tenant_type}")

        # Use first connection
        tenant_id = connections[0].tenant_id
        tenant_name = connections[0].tenant_name

        print(f"\n✓ Will use: {tenant_name} ({tenant_id})")

        # Add to token
        token_dict['tenant_id'] = tenant_id
        token_dict['tenant_name'] = tenant_name

        # Save
        token_string = json.dumps(token_dict, indent=2)
        dbutils.fs.put(token_file_path, token_string, overwrite=True)

        print(f"\n✓ Token updated successfully!")
        print(f"Tenant ID added: {tenant_id}")

    else:
        print("\n⚠ No organizations found via API")
        print("Try Option B: Manual Entry")

except Exception as e:
    print(f"\n⚠ Error: {str(e)}")
    print("\nThis error usually means:")
    print("1. Token is missing required scopes (openid, profile, email)")
    print("2. Token has expired")
    print("\nSolution: Run 05_Initial_Token_Setup.py to get a new token with correct scopes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Option B: Manual Entry

# CELL ********************

# If auto-detect failed, manually enter tenant_id
if tenant_id_input:
    print(f"Manually adding tenant_id: {tenant_id_input}")

    # Add to token
    token_dict['tenant_id'] = tenant_id_input

    # Save
    token_string = json.dumps(token_dict, indent=2)
    dbutils.fs.put(token_file_path, token_string, overwrite=True)

    print("\n✓ Token updated with manual tenant_id!")
else:
    print("\nTo manually add tenant_id:")
    print("1. Log in to https://go.xero.com/Settings/Organisation")
    print("2. Copy your Organization ID")
    print("3. Enter it in the 'tenant_id' widget above")
    print("4. Re-run this cell")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Verify

# CELL ********************

# Load and verify
token_content = dbutils.fs.head(token_file_path, 10000)
token_dict = json.loads(token_content)

print("Updated token info:")
print(f"  Has tenant_id: {'tenant_id' in token_dict}")
if 'tenant_id' in token_dict:
    print(f"  Tenant ID: {token_dict['tenant_id']}")
if 'tenant_name' in token_dict:
    print(f"  Tenant Name: {token_dict['tenant_name']}")

print("\n✓ Fix complete! You can now run the orchestrator.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dbutils.notebook.exit(json.dumps({
    'status': 'success',
    'has_tenant_id': 'tenant_id' in token_dict
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
