# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "574b6e94-7a6f-4b5d-be94-549b5adb6299",
# META       "default_lakehouse_name": "stLouisFedData",
# META       "default_lakehouse_workspace_id": "f0dd2f6f-1674-48ce-96a9-71e034725cde",
# META       "known_lakehouses": [
# META         {
# META           "id": "574b6e94-7a6f-4b5d-be94-549b5adb6299"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
from notebookutils import notebook
import json
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rootEndPoint = "https://api.stlouisfed.org/fred/series/observations"
apiKey = "01f5478a3ebc449cfeeb049c635c2bf4"
file_type = "json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

destinationFolder = "Files/" + "macro/fred/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

destinationSchema = "silver"
destinationTablePrefix = "fred_"
destinationSchemaTableprefix = destinationSchema + "." + destinationTablePrefix
destinationLoggingTable = "tabledataloadlogs" 


logTableSchema = StructType([
    StructField("seriesName",StringType()),
    StructField("seriesId", StringType()),
    StructField("tableName", StringType()),
    StructField("loadDate", TimestampType()),
    StructField("numberOfRows", IntegerType())
])

if notebookutils.fs.exists(f"Tables/{destinationSchema}/{destinationTablePrefix + destinationLoggingTable}") == False:
    print(f"Logging Table does not exist in the destination, Creating a new table {destinationSchemaTableprefix + destinationLoggingTable}")
    spark.createDataFrame([],logTableSchema).write.mode("overwrite").option("overwriteschema","true").format("delta").saveAsTable(f"{destinationSchemaTableprefix + destinationLoggingTable}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

requiredDocuments = [
    {
        "seriesName": "5y5y Inflation Expectations",
        "series_id": "T5YIFR",
        "tableName": "5y5yInflation"
    },
    {
        "seriesName": "CPI Services ex-Shelter (via CPI detail)",
        "series_id": "DSPIC96",
        "tableName": "CPIServices"
    },
    {
        "seriesName": "Nonfarm Payrolls (PAYEMS)",
        "series_id": "PAYEMS",
        "tableName": "NonfarmPayrolls"
    },
    {
        "seriesName": "Unemployment Rate (UNRATE)",
        "series_id": "UNRATE",
        "tableName": "UnemploymentRate"
    },
    {
        "seriesName": "Average Hourly Earnings (CES0500000003)",
        "series_id": "CES0500000003",
        "tableName": "AverageHourlyEarnings"
    },
    {
        "seriesName": "NYC Minimum Wage History",
        "series_id": "STTMINWGNY",
        "tableName": "NYCMinimumWage"
    },
    {
        "seriesName": "NJ Minimum Wage",
        "series_id": "STTMINWGNJ",
        "tableName": "NJMinimumWage"
    },
    {
        "seriesName": "Consumer Confidence Index (licensed)",
        "series_id": "UMCSENT",
        "tableName": "ConsumerConfidenceIndex"
    },
    {
        "seriesName": "Retail Sales - Control Group",
        "series_id": "RSXFS",
        "tableName": "RetailSalesControlGroup"
    },
    {
        "seriesName": "Average Hourly Earnings of All Employees, Leisure and Hospitality",
        "series_id": "CES7000000003",
        "tableName": "AverageHourlyEarningsOfAllEmployees"
    },
    {
        "seriesName": "Consumer Price Index for All Urban Consumers",
        "series_id": "CUSR0000SERF01",
        "tableName": "ConsumerPriceIndexForAllUrbanConsumers"
    },
    {
        "seriesName": "Average Price: Electricity per Kilowatt-Hour in New York-Newark-Jersey City, NY-NJ-PA (CBSA)",
        "series_id": "APUS12A72610",
        "tableName": "AveragePriceElectricityPerKilowattHour"
    },
    {
        "seriesName": "Producer Price Index by Industry: Plumbing, Heating and Air-Conditioning Contractors, Nonresidential Building Work",
        "series_id": "PCU23822X23822X",
        "tableName": "ProducerPriceIndexByIndustry"
    }
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def downloadData(urlParameters:str,fileName:str, tableName:str):
    try:
        apiEndpoint = f"{rootEndPoint}?{urlParameters}&api_key={apiKey}&file_type={file_type}"
        response = requests.get(apiEndpoint)
        if response.status_code == 200:
            rawDataJson = response.json()
            rawDataStr = json.dumps(rawDataJson)
            rawDataObservationsJson = rawDataJson.get("observations",{})
            fileName = fileName + ".json"
            tableName = destinationSchemaTableprefix + tableName
            notebookutils.fs.put(destinationFolder + fileName,rawDataStr,True)
            print(f"'{fileName}' data is downloaded")
            df = spark.createDataFrame(pd.DataFrame(rawDataObservationsJson))
            df.write.format("delta").option("overwriteschema","true").mode("overwrite").saveAsTable(tableName)
            spark.createDataFrame(pd.DataFrame({
                "seriesName":fileName,
                "seriesId":series_id,
                "tableName":tableName,
                "loadDate":datetime.datetime.now(),
                "numberOfRows":df.count()
            },index=[0]),schema=logTableSchema).write.mode("append").format("delta").saveAsTable(destinationSchemaTableprefix + destinationLoggingTable)
            print(f"Loaded into '{tableName}' table")
        else:
            print(f"Error while running the API {response.status_code}")
    except:
        print(f"Error while running for {fileName}") 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for document in requiredDocuments:
    series_id = document["series_id"]
    seriesName = document["seriesName"]
    tableName =  document["tableName"]
    downloadData(f"series_id={series_id}",seriesName,tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
