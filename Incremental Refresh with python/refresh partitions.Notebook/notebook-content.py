# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2b5829a3-6d3e-43a1-93b5-6db55408d918",
# META       "default_lakehouse_name": "incrementalRefreshTablePartitions",
# META       "default_lakehouse_workspace_id": "21ffc049-a83e-4c1f-b85b-c8e3a511b8bd",
# META       "known_lakehouses": [
# META         {
# META           "id": "2b5829a3-6d3e-43a1-93b5-6db55408d918"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType, DoubleType
import requests
import time
import math

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rootEndPoint = "https://api.powerbi.com/v1.0/myorg/"
datasetId= "03c01142-c4dc-44e0-8dd3-cde371eaef04"
workspaceId = "974cab78-dfc8-4b48-9662-ea4bd67eed64"
delay = 7200 #delay
batchCount = 3
bearerToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkhTMjNiN0RvN1RjYVUxUm9MSHdwSXEyNFZZZyIsImtpZCI6IkhTMjNiN0RvN1RjYVUxUm9MSHdwSXEyNFZZZyJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvZTcxNGVmMzEtZmFhYi00MWQyLTlmMWUtZTZkZjRhZjE2YWI4LyIsImlhdCI6MTc1OTE1MTQ3MSwibmJmIjoxNzU5MTUxNDcxLCJleHAiOjE3NTkxNTY2NTYsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBWFFBaS84YUFBQUFXSlFrM2hYYjczR2dzaU00OTAvcU9jZ0xydkpxbC9iU1dtNkFsbDVOTkRMT3ZYc3dCZ3MwdDFNY0tRMGhaUDRNRGtpUzY4TWxSMFVBbDNVbzVrK0JzbXBSMDZ4dHNFdk1CcDA0NnZhOGFhL1BpREV5TGVvZFlMNUZReGpHMGRodkU2WjRRM3JqemViU0p4SjJjekdySEE9PSIsImFtciI6WyJwd2QiLCJyc2EiLCJtZmEiXSwiYXBwaWQiOiIxOGZiY2ExNi0yMjI0LTQ1ZjYtODViMC1mN2JmMmIzOWIzZjMiLCJhcHBpZGFjciI6IjAiLCJkZXZpY2VpZCI6ImJhZTQyZTJiLTRiYmQtNGZmZS05NjY5LWFmYWM4YjMzMWNjMiIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjI0MDE6NDkwMDo4OGUwOmRkMGM6NDgyNjoxODFjOjQxZTc6NTZmYyIsIm5hbWUiOiJUaGFydW4gUmF2aWtyaW5kIiwib2lkIjoiNWNkYjA2ZTctNDRjYi00OTJjLTg0M2ItY2ExZDg0NGU5OWQzIiwicHVpZCI6IjEwMDMyMDAzNEVCMDVBMEYiLCJyaCI6IjEuQVZZQU1lOFU1NnY2MGtHZkh1YmZTdkZxdUFrQUFBQUFBQUFBd0FBQUFBQUFBQUNlQUpSV0FBLiIsInNjcCI6IkFwcC5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkV3JpdGUuQWxsIENvbm5lY3Rpb24uUmVhZC5BbGwgQ29ubmVjdGlvbi5SZWFkV3JpdGUuQWxsIENvbnRlbnQuQ3JlYXRlIERhc2hib2FyZC5SZWFkLkFsbCBEYXNoYm9hcmQuUmVhZFdyaXRlLkFsbCBEYXRhZmxvdy5SZWFkLkFsbCBEYXRhZmxvdy5SZWFkV3JpdGUuQWxsIERhdGFzZXQuUmVhZC5BbGwgRGF0YXNldC5SZWFkV3JpdGUuQWxsIEdhdGV3YXkuUmVhZC5BbGwgR2F0ZXdheS5SZWFkV3JpdGUuQWxsIEl0ZW0uRXhlY3V0ZS5BbGwgSXRlbS5FeHRlcm5hbERhdGFTaGFyZS5BbGwgSXRlbS5SZWFkV3JpdGUuQWxsIEl0ZW0uUmVzaGFyZS5BbGwgT25lTGFrZS5SZWFkLkFsbCBPbmVMYWtlLlJlYWRXcml0ZS5BbGwgUGlwZWxpbmUuRGVwbG95IFBpcGVsaW5lLlJlYWQuQWxsIFBpcGVsaW5lLlJlYWRXcml0ZS5BbGwgUmVwb3J0LlJlYWRXcml0ZS5BbGwgUmVwcnQuUmVhZC5BbGwgU3RvcmFnZUFjY291bnQuUmVhZC5BbGwgU3RvcmFnZUFjY291bnQuUmVhZFdyaXRlLkFsbCBUYWcuUmVhZC5BbGwgVGVuYW50LlJlYWQuQWxsIFRlbmFudC5SZWFkV3JpdGUuQWxsIFVzZXJTdGF0ZS5SZWFkV3JpdGUuQWxsIFdvcmtzcGFjZS5HaXRDb21taXQuQWxsIFdvcmtzcGFjZS5HaXRVcGRhdGUuQWxsIFdvcmtzcGFjZS5SZWFkLkFsbCBXb3Jrc3BhY2UuUmVhZFdyaXRlLkFsbCIsInNpZCI6IjAwOGJmNDM5LTY4ZDktYTZmZC04MmFjLWU3NTMyMWRkNDE0NSIsInNpZ25pbl9zdGF0ZSI6WyJrbXNpIl0sInN1YiI6InktRjJSV3BMRU1wRzFYRjlvVjBzUUkwcWlvQ2JidV9TYWVRSUlFd1NSZjAiLCJ0aWQiOiJlNzE0ZWYzMS1mYWFiLTQxZDItOWYxZS1lNmRmNGFmMTZhYjgiLCJ1bmlxdWVfbmFtZSI6InRoYXJ1bi5yYXZpa3JpbmRAdGlnZXJhbmFseXRpY3MuY29tIiwidXBuIjoidGhhcnVuLnJhdmlrcmluZEB0aWdlcmFuYWx5dGljcy5jb20iLCJ1dGkiOiJKWjVrNzQ3QjlFU09rT0lNU3FONEFBIiwidmVyIjoiMS4wIiwid2lkcyI6WyJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX2Z0ZCI6IjQ0R0NTZjh1QjZrT1VMZnhHR2t4RTFxdlVaTmZwaExrMVh2RndkUG9vQVlCYTI5eVpXRmpaVzUwY21Gc0xXUnpiWE0iLCJ4bXNfaWRyZWwiOiIxIDE4In0.PWGUDbag7V-w0r2cibJgd7evvnyKxiWXoWxu20NXZWKgco2xDn7nBmeOC-u1cOv90fQ00C3kDMF6DMQdbSyfxZ8TdjsK3PVzkFM9Hl954lmi7orNqOAU_i1xqhdK12Ro4tHiDZTkBvrd4v4USFjjmoexfmE4_HkTZqDOWY4mPL0HZ24_1mGcy4WmAUci01JlN9Nsq_31rDG6wiw-VkqWRTQ4kPqf8lrm4BRai0cPyP7I2XhLWr_psb17qbo1DzIH-oUxmhy6phrOZSnkLaEZ0klmWTOhjBkJtF7TMHUgipOQeqFDuwS5dxyztgmblCU0YdQajs7Irb-ArOlDbK4aVw"
headers = {
    "Authorization" : f"Bearer {bearerToken}",
    "Content-type" : "application/json"
}
payload = {
    "type": "Full",
    "commitMode": "transactional",
    "maxParallelism": 10,
    "retryCount": 2,
    "timeout": "02:00:00",
    "objects": [],
    "applyRefreshPolicy": False
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

partition_names = [
    "2022Q205", "2022Q206", "2022Q307", "2022Q308", "2022Q309", "2022Q410", "2022Q411", "2022Q412",
    "2023Q101", "2023Q102", "2023Q103", "2023Q204", "2023Q205", "2023Q206", "2023Q307", "2023Q308", "2023Q309", "2023Q410", "2023Q411", "2023Q412",
    "2024Q101", "2024Q102", "2024Q103", "2024Q204", "2024Q205", "2024Q206", "2024Q307", "2024Q308", "2024Q309", "2024Q410", "2024Q411", "2024Q412",
    "2025Q101", "2025Q102", "2025Q103", "2025Q204", "2025Q205", "2025Q206", "2025Q307", "2025Q308", "2025Q309"
]


today_str = datetime.today().strftime("%Y-%m-%d")

def get_calend_id(partition_name):
    year = partition_name[:4]
    mm = partition_name[-2:]
    return f"{year}{mm}"


all_dicts = []
for table_name in ["ChildKPIs", "ParentKPIs"]:
    for partition in partition_names:
        all_dicts.append({
            "TableName": table_name,
            "CALEND_ID_YYYYMM": get_calend_id(partition),
            "MAX_UPD_DT": today_str,
            "PartitionName": partition,
            "Refresh_Flag": "Y"
        })
df = pd.DataFrame(all_dicts)
df = spark.createDataFrame(df).write.format("delta").mode("overwrite").saveAsTable("dbo.partitions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df = spark.sql("SELECT * FROM incrementalRefreshTablePartitions.dbo.partitions where Refresh_Flag = 'Y'")
df = df.to_pandas_on_spark()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getrefreshStatus(datasetId,workspaceId):
    fullEndPoint = f"{rootEndPoint }groups/{workspaceId}/datasets/{datasetId}/refreshes"
    return requests.get(url=f"{fullEndPoint}?$top=1",headers=headers).json().get("value",{})[0].get("status","")

def postRefresh(datasetId,workspaceId,payload):
    fullEndPoint = f"{rootEndPoint }groups/{workspaceId}/datasets/{datasetId}/refreshes"
    response_code = requests.post(url=fullEndPoint,headers=headers,json=payload).status_code
    return response_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

batches = math.ceil(len(df) / batchCount)
status = getrefreshStatus(datasetId=datasetId, workspaceId=workspaceId)

for batch_num in range(batches):
    print(f"Call {batch_num}")
    objects = []
    temp = df.iloc[batch_num * batchCount : (batch_num + 1) * batchCount]
    for _, row in temp.iterrows():
        obj = {
            "table": row["TableName"],
            "partition": row["PartitionName"]
        }
        objects.append(obj)
    payload["objects"] = objects
    print(payload)
    try:
        if status in ["Completed", "Failed", "None"]:
            if postRefresh(datasetId=datasetId, workspaceId=workspaceId, payload=payload) == 202:
                status = getrefreshStatus(datasetId=datasetId, workspaceId=workspaceId)
                print(status)
                while status not in ["Completed", "Failed", "None"]:
                    print("Waiting while the refresh is running")
                    time.sleep(delay)
                    status = getrefreshStatus(datasetId=datasetId, workspaceId=workspaceId)
                    print(status)
    except Exception as e:
        print(f"Error while running the API: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
