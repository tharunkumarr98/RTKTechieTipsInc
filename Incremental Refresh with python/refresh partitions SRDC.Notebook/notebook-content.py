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
batchCount = 3
bearerToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkhTMjNiN0RvN1RjYVUxUm9MSHdwSXEyNFZZZyIsImtpZCI6IkhTMjNiN0RvN1RjYVUxUm9MSHdwSXEyNFZZZyJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvNDJjYzMyOTUtY2QwZS00NDljLWI5OGUtNWNlNWI1NjBjMWQzLyIsImlhdCI6MTc1OTE5Njg5NSwibmJmIjoxNzU5MTk2ODk1LCJleHAiOjE3NTkyMDIyNDIsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVVFBdS84WkFBQUFuTWZyeEd5UkRnTHQ0NndCL0piMWlUamZNS0h2Y3phQjQ5SnVmZ1BPRG0vMHczeHZzSlNscjgxN3BQY3o4ejZ2RTBDbEVPd1NsbUxPOWo3NkcrTXNFZz09IiwiYW1yIjpbInB3ZCJdLCJhcHBpZCI6IjE4ZmJjYTE2LTIyMjQtNDVmNi04NWIwLWY3YmYyYjM5YjNmMyIsImFwcGlkYWNyIjoiMCIsImNvbnRyb2xzIjpbImFwcF9yZXMiXSwiY29udHJvbHNfYXVkcyI6WyIwMDAwMDAwMy0wMDAwLTBmZjEtY2UwMC0wMDAwMDAwMDAwMDAiXSwiZmFtaWx5X25hbWUiOiJSYXZpa3JpbmRoaSIsImdpdmVuX25hbWUiOiJUaGFydW4iLCJpZHR5cCI6InVzZXIiLCJpcGFkZHIiOiIyNDAxOjQ5MDA6OGZjZjpjYjU1OjM0MGM6YTRhOTo1MGYzOmU3MzciLCJuYW1lIjoiUmF2aWtyaW5kaGksIFRoYXJ1biAtIENvbnRyYWN0b3Ige1BFUH0iLCJvaWQiOiJjNWUxYzVhZi1hOWRkLTQ5NTEtOTQ3NC00NDg0NzA1MzIwNGMiLCJvbnByZW1fc2lkIjoiUy0xLTUtMjEtMjQzMjc4NzYwMC0xMDMxODQ1MDMzLTM0MDM3Nzc4NzItMTUzMjM5MyIsInB1aWQiOiIxMDAzMjAwMzVENjc4MjdEIiwicmgiOiIxLkFRY0FsVExNUWc3Tm5FUzVqbHpsdFdEQjB3a0FBQUFBQUFBQXdBQUFBQUFBQUFBSEFPNEhBQS4iLCJzY3AiOiJBcHAuUmVhZC5BbGwgQ2FwYWNpdHkuUmVhZC5BbGwgQ2FwYWNpdHkuUmVhZFdyaXRlLkFsbCBDb25uZWN0aW9uLlJlYWQuQWxsIENvbm5lY3Rpb24uUmVhZFdyaXRlLkFsbCBDb250ZW50LkNyZWF0ZSBEYXNoYm9hcmQuUmVhZC5BbGwgRGFzaGJvYXJkLlJlYWRXcml0ZS5BbGwgRGF0YWZsb3cuUmVhZC5BbGwgRGF0YWZsb3cuUmVhZFdyaXRlLkFsbCBEYXRhc2V0LlJlYWQuQWxsIERhdGFzZXQuUmVhZFdyaXRlLkFsbCBHYXRld2F5LlJlYWQuQWxsIEdhdGV3YXkuUmVhZFdyaXRlLkFsbCBJdGVtLkV4ZWN1dGUuQWxsIEl0ZW0uRXh0ZXJuYWxEYXRhU2hhcmUuQWxsIEl0ZW0uUmVhZFdyaXRlLkFsbCBJdGVtLlJlc2hhcmUuQWxsIE9uZUxha2UuUmVhZC5BbGwgT25lTGFrZS5SZWFkV3JpdGUuQWxsIFBpcGVsaW5lLkRlcGxveSBQaXBlbGluZS5SZWFkLkFsbCBQaXBlbGluZS5SZWFkV3JpdGUuQWxsIFJlcG9ydC5SZWFkV3JpdGUuQWxsIFJlcHJ0LlJlYWQuQWxsIFN0b3JhZ2VBY2NvdW50LlJlYWQuQWxsIFN0b3JhZ2VBY2NvdW50LlJlYWRXcml0ZS5BbGwgVGFnLlJlYWQuQWxsIFRlbmFudC5SZWFkLkFsbCBUZW5hbnQuUmVhZFdyaXRlLkFsbCBVc2VyU3RhdGUuUmVhZFdyaXRlLkFsbCBXb3Jrc3BhY2UuR2l0Q29tbWl0LkFsbCBXb3Jrc3BhY2UuR2l0VXBkYXRlLkFsbCBXb3Jrc3BhY2UuUmVhZC5BbGwgV29ya3NwYWNlLlJlYWRXcml0ZS5BbGwiLCJzaWQiOiIwMDhkYzhmOS02YmRiLTYxNzYtYTBmNy1iNjk0Yzc3ZGRjZGUiLCJzdWIiOiJISXQxcVYxNFIzMkxENFEzVm0telRFbm4zR0NFSzZQT2Zyd1dMeG1nU1NZIiwidGlkIjoiNDJjYzMyOTUtY2QwZS00NDljLWI5OGUtNWNlNWI1NjBjMWQzIiwidW5pcXVlX25hbWUiOiJUaGFydW4uUmF2aWtyaW5kaGkuQ29udHJhY3RvckBwZXBzaWNvLmNvbSIsInVwbiI6IlRoYXJ1bi5SYXZpa3JpbmRoaS5Db250cmFjdG9yQHBlcHNpY28uY29tIiwidXRpIjoiVzk3ZXlFdEJBa2lGNzl4NHQ0UUVBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc19mdGQiOiJLeW5xY0NOS1FwZGl3QW5VZ05sSEhlcUJZQVh0cTVMNWdMUUpXcWpTOURrQmRYTnpiM1YwYUMxa2MyMXoiLCJ4bXNfaWRyZWwiOiIxIDIwIn0.b1CYOmnTUXe3e1q6paSum4TsFU743WH1ECLGpPkF7VPbNClac4WGGep00N-L1n9aAXubOxW0KxpgNQ0_xQFOx6_RxQv375My8cwsajAOStpC-ub5NcHsqSN7tgG9qxVXH61vb28vft0lORVmvu0dfwmoeGuZDybge-iTRwqQ5AGfCNSpBIIjAqDyJdQriOb_M5eS_YrkCwiCxdstb1gRlM1a__H5VpQ6fggJ9BjeHeiMZMl5b6jLAjovVlB4PJXa0IuoBSZquA9LGgBsG6qWY_0KxFu0HGXCDK0ECrNuVZhR-XMaQD7ZJeMzGIxFOslq81IsfCtMag-t0ZAYgFuu7g"
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
    "2025Q30901", "2025Q30902", "2025Q30903", "2025Q30904", "2025Q30905", "2025Q30906", "2025Q30907", "2025Q30908",
    "2025Q30909", "2025Q30910", "2025Q30911", "2025Q30912", "2025Q30913", "2025Q30914", "2025Q30915"
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
                print(f"Triggered Call {batch_num}, Current Refresh Status:{status}")
                while status not in ["Completed", "Failed", "None"]:
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

# CELL ********************

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def get_powerbi_partition_name(date_obj, granularity):
    year = date_obj.year
    month = date_obj.month
    quarter = (month - 1) // 3 + 1

    if granularity == "day":
        return f"{year}Q{quarter}{date_obj.strftime('%m%d')}"
    elif granularity == "month":
        return f"{year}Q{quarter}{date_obj.strftime('%m')}"
    elif granularity == "quarter":
        return f"{year}Q{quarter}"
    elif granularity == "year":
        return str(year)
    else:
        raise ValueError(f"Invalid granularity: {granularity}")


def add_granularity(date_obj, granularity, step=1):
    if granularity == "day":
        return date_obj + timedelta(days=step)
    elif granularity == "month":
        return date_obj + relativedelta(months=step)
    elif granularity == "quarter":
        return date_obj + relativedelta(months=3 * step)
    elif granularity == "year":
        return date_obj + relativedelta(years=step)
    else:
        raise ValueError(f"Invalid granularity: {granularity}")


def align_to_granularity_start(date_obj, granularity):
    if granularity == "day":
        return date_obj
    elif granularity == "month":
        return date_obj.replace(day=1)
    elif granularity == "quarter":
        start_month = ((date_obj.month - 1) // 3) * 3 + 1
        return date_obj.replace(month=start_month, day=1)
    elif granularity == "year":
        return date_obj.replace(month=1, day=1)
    else:
        raise ValueError(f"Invalid granularity: {granularity}")


def generate_partition_names(refresh_units: int,
                              archive_units: int,
                              granularity: str = "day",
                              reference_date: datetime = None):
    """
    Generate Power BI partition names based on incremental refresh policy.

    :param refresh_units: How many units (e.g., days, months) to refresh
    :param archive_units: Total archive period (in same units)
    :param granularity: "day", "month", "quarter", or "year"
    :param reference_date: Use a fixed date (default: today)
    :return: Tuple (all_partitions, partitions_to_refresh)
    """
    if reference_date is None:
        reference_date = datetime.today()

    reference_date = align_to_granularity_start(reference_date, granularity)
    start_date = add_granularity(reference_date, granularity, -archive_units + 1)
    refresh_start_date = add_granularity(reference_date, granularity, -refresh_units + 1)

    all_partitions = []
    refresh_partitions = []

    current = start_date
    while current <= reference_date:
        part_name = get_powerbi_partition_name(current, granularity)
        all_partitions.append(part_name)

        if current >= refresh_start_date:
            refresh_partitions.append(part_name)

        current = add_granularity(current, granularity)

    return all_partitions, refresh_partitions



all_parts, refresh_parts = generate_partition_names(refresh, archive, granularity)

print(f"\nAll Partitions ({granularity}):")
print(all_parts)

print(f"\nPartitions to Refresh ({granularity}):")
print(refresh_parts)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def get_powerbi_partition_name(date_obj, granularity):
    year = date_obj.year
    month = date_obj.month
    quarter = (month - 1) // 3 + 1

    if granularity == "day":
        return f"{year}Q{quarter}{date_obj.strftime('%m%d')}"
    elif granularity == "month":
        return f"{year}Q{quarter}{date_obj.strftime('%m')}"
    elif granularity == "quarter":
        return f"{year}Q{quarter}"
    elif granularity == "year":
        return str(year)
    else:
        raise ValueError(f"Invalid granularity: {granularity}")


def align_to_granularity_start(date_obj, granularity):
    if granularity == "day":
        return date_obj
    elif granularity == "month":
        return date_obj.replace(day=1)
    elif granularity == "quarter":
        month = ((date_obj.month - 1) // 3) * 3 + 1
        return date_obj.replace(month=month, day=1)
    elif granularity == "year":
        return date_obj.replace(month=1, day=1)
    else:
        raise ValueError(f"Invalid granularity: {granularity}")


def add_granularity(date_obj, granularity, step=1):
    if granularity == "day":
        return date_obj + timedelta(days=step)
    elif granularity == "month":
        return date_obj + relativedelta(months=step)
    elif granularity == "quarter":
        return date_obj + relativedelta(months=3 * step)
    elif granularity == "year":
        return date_obj + relativedelta(years=step)
    else:
        raise ValueError(f"Invalid granularity: {granularity}")


def generate_partition_names(archive_number: int,
                             archive_granularity: str,
                             refresh_number: int,
                             refresh_granularity: str,
                             effective_date: datetime = None):
    """
    Generate partition names following Power BI incremental refresh logic.

    :param archive_number: How far back to retain data (e.g., 5)
    :param archive_granularity: Unit of archive window (e.g., 'year')
    :param refresh_number: Number of units to refresh (e.g., 2)
    :param refresh_granularity: Partitioning granularity (e.g., 'month')
    :param effective_date: The "today" date for refresh logic
    :return: (all_partitions, refresh_partitions)
    """
    if effective_date is None:
        effective_date = datetime.today()

    # Calculate the earliest archive date
    archive_start = align_to_granularity_start(
        add_granularity(effective_date, archive_granularity, -archive_number + 1),
        refresh_granularity
    )

    # Align effective date to start of refresh granularity
    partition_end = align_to_granularity_start(effective_date, refresh_granularity)

    # Calculate refresh start date
    refresh_start = add_granularity(partition_end, refresh_granularity, -refresh_number + 1)

    all_partitions = []
    refresh_partitions = []

    current = archive_start
    while current <= partition_end:
        part_name = get_powerbi_partition_name(current, refresh_granularity)
        all_partitions.append(part_name)

        if current >= refresh_start:
            refresh_partitions.append(part_name)

        current = add_granularity(current, refresh_granularity)

    return all_partitions, refresh_partitions


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_parts, refresh_parts = generate_partition_names(
        archive_number=6,
        archive_granularity="year",
        refresh_number=3,
        refresh_granularity="month",
        effective_date=datetime(2025, 9, 30)
    )

print(all_parts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

GRANULARITY_ORDER = ['year', 'quarter', 'month', 'day']

def get_partition_name(date_obj, granularity):
    year = date_obj.year
    month = date_obj.month
    quarter = (month - 1) // 3 + 1

    if granularity == "year":
        return f"{year}"
    elif granularity == "quarter":
        return f"{year}Q{quarter}"
    elif granularity == "month":
        return f"{year}Q{quarter}{date_obj.strftime('%m')}"
    elif granularity == "day":
        return f"{year}Q{quarter}{date_obj.strftime('%m%d')}"
    else:
        raise ValueError(f"Unsupported granularity: {granularity}")


def add_granularity(date_obj, granularity, step=1):
    if granularity == "day":
        return date_obj + timedelta(days=step)
    elif granularity == "month":
        return date_obj + relativedelta(months=step)
    elif granularity == "quarter":
        return date_obj + relativedelta(months=3 * step)
    elif granularity == "year":
        return date_obj + relativedelta(years=step)
    else:
        raise ValueError(f"Unsupported granularity: {granularity}")


def align_to_granularity_start(date_obj, granularity):
    if granularity == "day":
        return date_obj
    elif granularity == "month":
        return date_obj.replace(day=1)
    elif granularity == "quarter":
        start_month = ((date_obj.month - 1) // 3) * 3 + 1
        return date_obj.replace(month=start_month, day=1)
    elif granularity == "year":
        return date_obj.replace(month=1, day=1)
    else:
        raise ValueError(f"Unsupported granularity: {granularity}")


def generate_smart_partitions(archive_number: int,
                               archive_granularity: str,
                               refresh_number: int,
                               refresh_granularity: str,
                               effective_date: datetime):
    """
    Generate smart partitions using mixed granularity based on Power BI-like incremental refresh behavior.
    """
    if GRANULARITY_ORDER.index(archive_granularity) > GRANULARITY_ORDER.index(refresh_granularity):
        raise ValueError("Archive granularity must be coarser than or equal to refresh granularity.")

    # Start of archive window
    archive_start = align_to_granularity_start(
        add_granularity(effective_date, archive_granularity, -archive_number + 1),
        archive_granularity
    )

    # Start of refresh window
    refresh_start = align_to_granularity_start(
        add_granularity(effective_date, refresh_granularity, -refresh_number + 1),
        refresh_granularity
    )

    partitions = []

    # Generate partitions from archive_start to refresh_start using increasingly fine granularities
    current = archive_start
    while current < refresh_start:
        for gran in GRANULARITY_ORDER:
            if GRANULARITY_ORDER.index(gran) < GRANULARITY_ORDER.index(refresh_granularity):
                next = add_granularity(current, gran)
                if next <= refresh_start:
                    part_name = get_partition_name(current, gran)
                    partitions.append(part_name)
                    current = next
                    break
        else:
            # fallback to refresh granularity if no coarser fits
            break

    # Generate fine-grained partitions from refresh_start to effective_date
    current = refresh_start
    while current <= effective_date:
        part_name = get_partition_name(current, refresh_granularity)
        partitions.append(part_name)
        current = add_granularity(current, refresh_granularity)

    return partitions


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_parts = generate_smart_partitions(
        archive_number=6,
        archive_granularity="year",
        refresh_number=2,
        refresh_granularity="day",
        effective_date=datetime(2025, 9, 30)
    )

print(all_parts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def get_partition_name(date_obj, granularity):
    year = date_obj.year
    month = date_obj.month
    quarter = (month - 1) // 3 + 1

    if granularity == "year":
        return f"{year}"
    elif granularity == "quarter":
        return f"{year}Q{quarter}"
    elif granularity == "month":
        return f"{year}Q{quarter}{date_obj.strftime('%m')}"
    elif granularity == "day":
        return f"{year}Q{quarter}{date_obj.strftime('%m%d')}"
    else:
        raise ValueError(f"Invalid granularity: {granularity}")


def generate_powerbi_style_partitions(archive_number, archive_granularity, refresh_number, refresh_granularity, effective_date):
    partitions = []

    # Calculate key dates
    archive_start = datetime(effective_date.year, 1, 1) - relativedelta(years=archive_number - 1)
    refresh_start = effective_date - relativedelta(**{f"{refresh_granularity}s": refresh_number - 1})
    refresh_start = refresh_start.replace(hour=0, minute=0, second=0, microsecond=0)

    current = archive_start

    # 1. Add yearly partitions up to the year before refresh_start
    while current.year < refresh_start.year:
        partitions.append(get_partition_name(current, "year"))
        current += relativedelta(years=1)

    # 2. Add quarterly partitions in refresh_start year up to quarter before refresh_start
    while (current.year < refresh_start.year) or \
          (current.year == refresh_start.year and ((current.month - 1)//3 + 1) < ((refresh_start.month - 1)//3 + 1)):
        partitions.append(get_partition_name(current, "quarter"))
        current += relativedelta(months=3)

    # 3. Add monthly partitions for the months before the last full month
    last_full_month = effective_date.replace(day=1)
    month_iter = current.replace(day=1)
    while month_iter < last_full_month:
        partitions.append(get_partition_name(month_iter, "month"))
        month_iter += relativedelta(months=1)

    # 4. Add daily partitions for the final full month (i.e., September 2025)
    day_iter = last_full_month
    while day_iter <= effective_date:
        partitions.append(get_partition_name(day_iter, "day"))
        day_iter += timedelta(days=1)

    return partitions


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

partitions = generate_powerbi_style_partitions(
    archive_number=3,
    archive_granularity="year",
    refresh_number=2,
    refresh_granularity="month",
    effective_date=datetime(2025, 9, 30)
)

for p in partitions:
    print(p)

print(f"\nTotal Partitions: {len(partitions)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def get_partition_name(date_obj, granularity):
    year = date_obj.year
    month = date_obj.month
    quarter = (month - 1) // 3 + 1

    if granularity == "year":
        return f"{year}"
    elif granularity == "quarter":
        return f"{year}Q{quarter}"
    elif granularity == "month":
        return f"{year}Q{quarter}{date_obj.strftime('%m')}"
    elif granularity == "day":
        return f"{year}Q{quarter}{date_obj.strftime('%m%d')}"
    else:
        raise ValueError("Invalid granularity")


def generate_smart_powerbi_partitions(
    archive_number,
    archive_granularity,
    refresh_number,
    refresh_granularity,
    effective_date
):
    partitions = []

    # Determine boundaries
    refresh_start = effective_date - relativedelta(**{f"{refresh_granularity}s": refresh_number - 1})
    refresh_start = refresh_start.replace(hour=0, minute=0, second=0, microsecond=0)

    archive_start = effective_date.replace(month=1, day=1) - relativedelta(**{f"{archive_granularity}s": archive_number - 1})
    current = archive_start

    # Step 1: ARCHIVE PERIOD — partition everything < refresh_start using "coarsest" possible, but aligned to year/month
    while current < refresh_start:
        if current.month == 1 and (current + relativedelta(years=1)) <= refresh_start:
            # Full year fits before refresh_start
            partitions.append(get_partition_name(current, "year"))
            current += relativedelta(years=1)

        elif current.month in [1, 4, 7, 10] and (current + relativedelta(months=3)) <= refresh_start:
            # Full quarter fits
            partitions.append(get_partition_name(current, "quarter"))
            current += relativedelta(months=3)

        elif (current + relativedelta(months=1)) <= refresh_start:
            # Only month fits
            partitions.append(get_partition_name(current, "month"))
            current += relativedelta(months=1)

        else:
            break  # exit if we can't fit anything before refresh_start

    # Step 2: REFRESH PERIOD — partition exactly by refresh_granularity
    current = refresh_start
    while current <= effective_date:
        partitions.append(get_partition_name(current, refresh_granularity))

        if refresh_granularity == "day":
            current += timedelta(days=1)
        elif refresh_granularity == "month":
            current += relativedelta(months=1)
        elif refresh_granularity == "quarter":
            current += relativedelta(months=3)
        elif refresh_granularity == "year":
            current += relativedelta(years=1)
        else:
            raise ValueError("Invalid granularity")

    return partitions


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

partitions = generate_smart_powerbi_partitions(
    archive_number=4,
    archive_granularity="month",
    refresh_number=2,
    refresh_granularity="month",
    effective_date=datetime(2025, 9, 30)
)

for p in partitions:
    print(p)

print(f"\nTotal partitions: {len(partitions)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
