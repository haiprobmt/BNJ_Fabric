# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Xero Authentication Module
# # MAGIC
# This notebook handles OAuth2 authentication with Xero API.
# # MAGIC
# **Note:** For production use in Fabric, tokens should be stored in Azure Key Vault.

# MARKDOWN ********************

# ## 1. Import Libraries

# CELL ********************

import requests
import json
import os
from datetime import datetime, timedelta
from xero_python.api_client import ApiClient
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.accounting import AccountingApi
from xero_python.identity import IdentityApi

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Get Parameters

# CELL ********************

# Get parameters from widgets or previous notebook
try:
    client_id = dbutils.widgets.get("client_id")
    client_secret = dbutils.widgets.get("client_secret")
    redirect_uri = dbutils.widgets.get("redirect_uri")
    token_file_path = dbutils.widgets.get("token_file_path")
except:
    # Fallback if widgets not set
    client_id = ""
    client_secret = ""
    redirect_uri = "http://localhost:3000"
    token_file_path = "/lakehouse/default/Files/config/xero_token.json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Token Management Functions

# CELL ********************

def save_token_to_lakehouse(token_dict, file_path):
    """Save token to lakehouse file system"""
    token_data = token_dict.copy()
    token_data['saved_at'] = datetime.utcnow().isoformat()

    # Convert to JSON string
    token_json = json.dumps(token_data, indent=2)

    # Save to lakehouse
    dbutils.fs.put(file_path, token_json, overwrite=True)
    print(f"Token saved to {file_path}")

def load_token_from_lakehouse(file_path):
    """Load token from lakehouse file system"""
    try:
        # Read from lakehouse
        token_content = dbutils.fs.head(file_path, 10000)
        token_dict = json.loads(token_content)
        print(f"Token loaded from {file_path}")
        return token_dict
    except Exception as e:
        print(f"No token found: {e}")
        return None

def is_token_expired(token_dict):
    """Check if token is expired"""
    if not token_dict:
        return True

    if 'expires_in' not in token_dict or 'saved_at' not in token_dict:
        return True

    saved_at = datetime.fromisoformat(token_dict['saved_at'])
    expires_in = token_dict['expires_in']
    expiration_time = saved_at + timedelta(seconds=expires_in)
    buffer_time = datetime.utcnow() + timedelta(minutes=5)

    return buffer_time >= expiration_time

def refresh_access_token(token_dict, client_id, client_secret):
    """Refresh the access token using refresh token"""
    if not token_dict or 'refresh_token' not in token_dict:
        raise Exception("No refresh token available")

    token_url = 'https://identity.xero.com/connect/token'
    token_data = {
        'grant_type': 'refresh_token',
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': token_dict['refresh_token']
    }

    response = requests.post(token_url, data=token_data)
    response.raise_for_status()
    new_token = response.json()

    print("Token refreshed successfully!")
    return new_token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Initialize Xero API Client

# CELL ********************

class XeroFabricConnector:
    """Xero connector for Fabric notebooks"""

    def __init__(self, client_id, client_secret, token_file_path):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_file_path = token_file_path
        self.stored_token = None
        self.api_client = None
        self.tenant_id = None

    def ensure_valid_token(self):
        """Ensure we have a valid access token"""
        # Load token from lakehouse
        self.stored_token = load_token_from_lakehouse(self.token_file_path)

        if not self.stored_token:
            raise Exception(
                "No token found. Please run initial authentication. "
                "For Fabric, tokens should be pre-configured or obtained via service principal."
            )

        # Check if token is expired
        if is_token_expired(self.stored_token):
            print("Token expired. Refreshing...")
            self.stored_token = refresh_access_token(
                self.stored_token,
                self.client_id,
                self.client_secret
            )
            save_token_to_lakehouse(self.stored_token, self.token_file_path)
        else:
            print("Using existing valid token.")

        # Set up API client
        self._setup_api_client()

        return self.stored_token

    def _setup_api_client(self):
        """Set up the Xero API client"""
        oauth2_token = OAuth2Token(
            client_id=self.client_id,
            client_secret=self.client_secret
        )

        oauth2_token.update_token(
            access_token=self.stored_token.get('access_token'),
            scope=self.stored_token.get('scope', ''),
            expires_in=self.stored_token.get('expires_in'),
            token_type=self.stored_token.get('token_type', 'Bearer'),
            refresh_token=self.stored_token.get('refresh_token'),
            id_token=self.stored_token.get('id_token')
        )

        self.api_client = ApiClient(
            Configuration(oauth2_token=oauth2_token),
            pool_threads=1
        )

        # Set up token callbacks
        connector_ref = self

        @self.api_client.oauth2_token_getter
        def obtain_xero_oauth2_token():
            token = connector_ref.stored_token.copy()
            # Remove custom fields that OAuth2Token doesn't expect
            token.pop('saved_at', None)
            token.pop('tenant_id', None)
            token.pop('setup_type', None)
            return token

        @self.api_client.oauth2_token_saver
        def store_xero_oauth2_token(token):
            connector_ref.stored_token = token
            save_token_to_lakehouse(token, connector_ref.token_file_path)

    def get_accounting_api(self):
        """Get AccountingApi instance"""
        if not self.api_client:
            raise Exception("API client not initialized")
        return AccountingApi(self.api_client)

    def get_tenant_id(self):
        """Get tenant (organization) ID"""
        if self.tenant_id:
            return self.tenant_id

        try:
            identity_api = IdentityApi(self.api_client)
            connections = identity_api.get_connections()

            print(f"Found {len(connections)} Xero connections")

            if not connections:
                # Try alternative method - use stored tenant_id from token if available
                if self.stored_token and 'tenant_id' in self.stored_token:
                    self.tenant_id = self.stored_token['tenant_id']
                    print(f"Using tenant_id from stored token: {self.tenant_id}")
                    return self.tenant_id

                raise Exception(
                    "No Xero organizations found. This may be due to missing scopes. "
                    "Required scopes: 'openid profile email accounting.transactions accounting.settings accounting.reports.read offline_access'"
                )

            # Use the first connection
            self.tenant_id = connections[0].tenant_id

            # Store tenant_id in token for future use
            if self.stored_token:
                self.stored_token['tenant_id'] = self.tenant_id
                save_token_to_lakehouse(self.stored_token, self.token_file_path)

            print(f"Connected to: {connections[0].tenant_name}")
            print(f"Tenant ID: {self.tenant_id}")

            return self.tenant_id

        except Exception as e:
            print(f"Error getting tenant ID: {str(e)}")
            print("\nTroubleshooting:")
            print("1. Ensure token has required scopes: 'openid profile email'")
            print("2. Re-run 05_Initial_Token_Setup.py with correct scopes")
            print("3. Verify you have access to at least one Xero organization")
            raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Initialize Connector

# CELL ********************

# Initialize the connector
connector = XeroFabricConnector(client_id, client_secret, token_file_path)

# Ensure valid token
connector.ensure_valid_token()

# Get API instances
accounting_api = connector.get_accounting_api()
tenant_id = connector.get_tenant_id()

print(f"Connected to Xero Tenant: {tenant_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return connector object for use in other notebooks
dbutils.notebook.exit("Authentication successful")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
