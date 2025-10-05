# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

import json
import time
import requests
import datetime

client_id = dbutils.secrets.get(scope="pbna-df-mra", key="pbna-df-spn-client")
client_secret = dbutils.secrets.get(scope="pbna-df-mra", key="pbna-df-spn-secret")
tenant_id = dbutils.secrets.get(scope="pbna-df-mra", key="pbna-dfn-dev-spn-tenantid")
base_url = f"https://api.powerbi.com/v1.0/myorg/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_accessToken(client_id, client_secret, tenant_id):
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": "https://analysis.windows.net/powerbi/api",
    }
     
    response = requests.post(token_url, data=data)
 
    if response.status_code != 200:
        response.raise_for_status()

    token_data = response.json()
    return token_data.get("access_token")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def RefreshDatasetPartition(workspace_id, dataset_id, base_url, headers, payload):
    relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    response = requests.post(relative_url, headers=headers, json=payload)
 
    if response.status_code == 202:
        print(f"Dataset {dataset_name} refresh has been triggered successfully.")
    else:
        print(f"Failed to trigger dataset {dataset_name} refresh.")
    return response.status_code, response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def CheckRefreshStatus(workspace_id, dataset_id, base_url, headers, check_every_seconds=120):
    relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"

    refresh_status = "started"
    while refresh_status != "Completed" and refresh_status != "Failed":
        try:
            response = requests.get(relative_url, headers=headers)
            if response.status_code == 401:  
                print("Token expired, generating a new one...")
                headers["Authorization"] = "Bearer " + get_accessToken()
                continue
            response.raise_for_status()
            last_refresh = response.json()["value"][0]
            refresh_status = last_refresh["status"]
            print("status - " + refresh_status + " " + str(datetime.datetime.now()))
            if refresh_status != "Completed" and refresh_status != "Failed":
                time.sleep(check_every_seconds)
        except Exception as e:
            print(f"Error while checking refresh status: {e}")
            time.sleep(10)
    return last_refresh

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payloadList = [
    # Parent KPIs
    {
        "type": "full",
        "applyRefreshPolicy": True,
        "objects": [
            {
                "table": "Parent KPIs"
            }
        ]
    },
    # Child KPIs
    {
        "type": "full",
        "applyRefreshPolicy": True,
        "objects": [
            {
                "table": "Child KPIs"
            }
        ]
    },
    # All other tables
    {
        "type": "full",
        "applyRefreshPolicy": True,
        "objects": [
            {"table": "Donated Flag"},
            {"table": "Package Category"},
            {"table": "Business Category"},
            {"table": "Restatement Flag"},
            {"table": "Time"},
            {"table": "Time Series"},
            {"table": "Sales Geography"},
            {"table": "Customer"},
            {"table": "Business Statement"},
            {"table": "Product"},
            {"table": "Partnershi Brand Groups"},
            {"table": "Data Refresh"}
        ]
    }
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = '74a289cb-2b89-481b-b2e2-923be476cfeb'
dataset_id = 'f65285c1-0f16-4731-8ff7-322bbd6c0b61'
dataset_name = 'PBNA SAP Daily Sales (Cockpit - 3.0)'

for payload in payloadList:
  access_token = get_accessToken(client_id, client_secret, tenant_id)

  headers = {
      "Authorization": f"Bearer {access_token}",
      "Content-Type": "application/json"
  }

  refresh_status_code, refresh_response = RefreshDatasetPartition(workspace_id, dataset_id, base_url, headers, payload)
  response = CheckRefreshStatus(workspace_id, dataset_id, base_url, headers)
  print(response["status"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dbutils.NotebookHandler.exit(response["status"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
