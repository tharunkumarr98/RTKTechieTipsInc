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
datasetId= "7e6dc9a0-f435-43d4-84ef-b745b2af1bc3"
workspaceId = "760c32a2-2681-4030-af00-16ff1a94bcd7"
delay = 180 #delay
batchSize = 3
bearerToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkhTMjNiN0RvN1RjYVUxUm9MSHdwSXEyNFZZZyIsImtpZCI6IkhTMjNiN0RvN1RjYVUxUm9MSHdwSXEyNFZZZyJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvNDJjYzMyOTUtY2QwZS00NDljLWI5OGUtNWNlNWI1NjBjMWQzLyIsImlhdCI6MTc1OTMyNjM4OCwibmJmIjoxNzU5MzI2Mzg4LCJleHAiOjE3NTkzMzExNzksImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVVFBdS84WkFBQUF5MDFWNjBsU2QyR1AyQnBoNHhqRytlOWxkOEtHQ2ExdmxiN2pxM2lBU0pNa0RtK2dxOCtyODNPdVJ1dWFZenBEaTEzb21yaVhqREdxbGxveFBjbEhsUT09IiwiYW1yIjpbInB3ZCJdLCJhcHBpZCI6IjE4ZmJjYTE2LTIyMjQtNDVmNi04NWIwLWY3YmYyYjM5YjNmMyIsImFwcGlkYWNyIjoiMCIsImNvbnRyb2xzIjpbImFwcF9yZXMiXSwiY29udHJvbHNfYXVkcyI6WyIwMDAwMDAwMy0wMDAwLTBmZjEtY2UwMC0wMDAwMDAwMDAwMDAiXSwiZmFtaWx5X25hbWUiOiJSYXZpa3JpbmRoaSIsImdpdmVuX25hbWUiOiJUaGFydW4iLCJpZHR5cCI6InVzZXIiLCJpcGFkZHIiOiIyNDAxOjQ5MDA6OGZjYzphMWQ6NjhlMzpkYzE5OmU0ZjpjYjZhIiwibmFtZSI6IlJhdmlrcmluZGhpLCBUaGFydW4gLSBDb250cmFjdG9yIHtQRVB9Iiwib2lkIjoiYzVlMWM1YWYtYTlkZC00OTUxLTk0NzQtNDQ4NDcwNTMyMDRjIiwib25wcmVtX3NpZCI6IlMtMS01LTIxLTI0MzI3ODc2MDAtMTAzMTg0NTAzMy0zNDAzNzc3ODcyLTE1MzIzOTMiLCJwdWlkIjoiMTAwMzIwMDM1RDY3ODI3RCIsInJoIjoiMS5BUWNBbFRMTVFnN05uRVM1amx6bHRXREIwd2tBQUFBQUFBQUF3QUFBQUFBQUFBQUhBTzRIQUEuIiwic2NwIjoiQXBwLlJlYWQuQWxsIENhcGFjaXR5LlJlYWQuQWxsIENhcGFjaXR5LlJlYWRXcml0ZS5BbGwgQ29ubmVjdGlvbi5SZWFkLkFsbCBDb25uZWN0aW9uLlJlYWRXcml0ZS5BbGwgQ29udGVudC5DcmVhdGUgRGFzaGJvYXJkLlJlYWQuQWxsIERhc2hib2FyZC5SZWFkV3JpdGUuQWxsIERhdGFmbG93LlJlYWQuQWxsIERhdGFmbG93LlJlYWRXcml0ZS5BbGwgRGF0YXNldC5SZWFkLkFsbCBEYXRhc2V0LlJlYWRXcml0ZS5BbGwgR2F0ZXdheS5SZWFkLkFsbCBHYXRld2F5LlJlYWRXcml0ZS5BbGwgSXRlbS5FeGVjdXRlLkFsbCBJdGVtLkV4dGVybmFsRGF0YVNoYXJlLkFsbCBJdGVtLlJlYWRXcml0ZS5BbGwgSXRlbS5SZXNoYXJlLkFsbCBPbmVMYWtlLlJlYWQuQWxsIE9uZUxha2UuUmVhZFdyaXRlLkFsbCBQaXBlbGluZS5EZXBsb3kgUGlwZWxpbmUuUmVhZC5BbGwgUGlwZWxpbmUuUmVhZFdyaXRlLkFsbCBSZXBvcnQuUmVhZFdyaXRlLkFsbCBSZXBydC5SZWFkLkFsbCBTdG9yYWdlQWNjb3VudC5SZWFkLkFsbCBTdG9yYWdlQWNjb3VudC5SZWFkV3JpdGUuQWxsIFRhZy5SZWFkLkFsbCBUZW5hbnQuUmVhZC5BbGwgVGVuYW50LlJlYWRXcml0ZS5BbGwgVXNlclN0YXRlLlJlYWRXcml0ZS5BbGwgV29ya3NwYWNlLkdpdENvbW1pdC5BbGwgV29ya3NwYWNlLkdpdFVwZGF0ZS5BbGwgV29ya3NwYWNlLlJlYWQuQWxsIFdvcmtzcGFjZS5SZWFkV3JpdGUuQWxsIiwic2lkIjoiMDA5OGM1NzktN2U1MS04NmQ2LTYxYTMtOWMwM2EwMTY5Y2YxIiwic3ViIjoiSEl0MXFWMTRSMzJMRDRRM1ZtLXpURW5uM0dDRUs2UE9mcndXTHhtZ1NTWSIsInRpZCI6IjQyY2MzMjk1LWNkMGUtNDQ5Yy1iOThlLTVjZTViNTYwYzFkMyIsInVuaXF1ZV9uYW1lIjoiVGhhcnVuLlJhdmlrcmluZGhpLkNvbnRyYWN0b3JAcGVwc2ljby5jb20iLCJ1cG4iOiJUaGFydW4uUmF2aWtyaW5kaGkuQ29udHJhY3RvckBwZXBzaWNvLmNvbSIsInV0aSI6IjJRSlpxWjYzamsycXRqbGU1UEE0QUEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbImI3OWZiZjRkLTNlZjktNDY4OS04MTQzLTc2YjE5NGU4NTUwOSJdLCJ4bXNfZnRkIjoiTDNSNXI5MGgzT0luSmgzR2RKaVVPOHpwT1FwcTZQX1NORjZmcGlYb2ljZ0JkWE56YjNWMGFDMWtjMjF6IiwieG1zX2lkcmVsIjoiMSAxNCJ9.ad7TJIE01R8xHZbFYHY_Yo71X-FG3j7K2ankDJ7-Ondn1oIjlLopxDavE783sSl2rt-bgTVOW_XuHilYX1Wd9VCh904RZ09UgndOCjB9G9XQzIxpzqE3IzvezMGBFEsExRsuQVpsQhWRjLkPB2rMQMaZcUgJr1JVVWGxLmDzGwlD-30e6gn7A3mR_htpgDTlmqTHI-ffci6uecAMT8qOacOIlo7vpXgpmOXgD-kjSaUwaOa-oAdwZQ22yOXNJIdB7Y5qPNP5vNjp2JOD45ORmVb2GwnfDtzdTLtWd79QDwYeaDVHQ69pwSs-vHZA6OZDX619i5bRCfq4Sgc9VZlmHQ"
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
    "2024",  "2025Q1", "2025Q2", "2025Q3", "2025Q41001"
]


today_str = datetime.today().strftime("%Y-%m-%d")

def get_calend_id(partition_name):
    year = partition_name[:4]
    mm = partition_name[-4:-2]
    return f"{year}{mm}"


all_dicts = []
for table_name in ["FactDirectTasks"]:
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
df = df.pandas_api()

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

batches = math.ceil(len(df) / batchSize)
status = getrefreshStatus(datasetId=datasetId, workspaceId=workspaceId)

for batch_num in range(batches):
    print(f"Call {batch_num}")
    objects = []
    temp = df.iloc[batch_num * batchSize : (batch_num + 1) * batchSize]
    for _, row in temp.iterrows():
        obj = {
            "table": row["TableName"],
            "partition": row["PartitionName"]
        }
        objects.append(obj)
    payload["objects"] = objects
    print(payload)
    try:
        if status in ["Completed", "Failed", "None","Cancelled"]:
            if postRefresh(datasetId=datasetId, workspaceId=workspaceId, payload=payload) == 202:
                status = getrefreshStatus(datasetId=datasetId, workspaceId=workspaceId)
                print(f"Triggered Call {batch_num}, Current Refresh Status:{status}")
                while status not in ["Completed", "Failed", "None","Cancelled"]:
                    print("Waiting while the refresh is running")
                    time.sleep(delay)
                    status = getrefreshStatus(datasetId=datasetId, workspaceId=workspaceId)
                    print(f"Current Refresh Status:{status}")
    except Exception as e:
        print(f"Error while running the API: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
