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
import json
import datetime
import pandas as pd
from notebookutils import notebook
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# #### Source API Parameters

# CELL ********************

# API details
api_endpoint_url = "https://api.stlouisfed.org/fred/series/observations"
api_key = "01f5478a3ebc449cfeeb049c635c2bf4"
file_type = "json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Bronze layer

# MARKDOWN ********************

# Initializes destination locations in Bronze Layer

# CELL ********************

# Destination details
destination_files_folder = "Files/macro/fred/"
destination_schema = "dbo" 
destination_table_prefix = "fred_"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Creates Data Load History table if not already

# CELL ********************

logging_table_name = destination_table_prefix + "data_loadhistory"
destination_logging_table = destination_schema + "." + logging_table_name

# Logging table schema
logTableSchema = StructType([
    StructField("seriesName", StringType()),
    StructField("seriesId", StringType()),
    StructField("tableName", StringType()),
    StructField("loadDate", TimestampType()),
    StructField("numberOfRows", IntegerType())
])

if notebookutils.fs.exists(f"Tables/{destination_schema}/{logging_table_name}") == False:
    print(f"Creating logging table {destination_logging_table}")
    spark.createDataFrame([], logTableSchema).write.mode("overwrite").format("delta").saveAsTable(
        f"{destination_logging_table}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Defines Series details

# CELL ********************

requiredDocuments = [
    {
        "seriesName": "5-Year, 5-Year Forward Inflation Expectation Rate",
        "series_id": "T5YIFR",
        "tableName": "5y5yInflation",
        "seriesDisplayName": "5y5y Inflation Expectations",
        "formatString":"0.00",
        "units":"Percent"
    },
    {
        "seriesName": "Real Disposable Personal Income",
        "series_id": "DSPIC96",
        "tableName": "RealDisposablePersonalIncome",
        "seriesDisplayName": "Real Disposable income",
        "formatString":"#,0.0",
        "units":"Billions of Chained 2017 Dollars"
    },
    {
        "seriesName": "Consumer Price Index for All Urban Consumers: Services Less Rent of Shelter in U.S. City Average",
        "series_id": "CUSR0000SASL2RS",
        "tableName": "CPIServices",
        "seriesDisplayName": "CPI Services ex-Shelter",
        "formatString":"#,0.000",
        "units":"Index Dec 1982=100"
    },
    {
        "seriesName": "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average",
        "series_id": "CPIAUCSL",
        "tableName": "CPIAllItems",
        "seriesDisplayName": "CPI All Urban Consumers: All Items in US Average",
        "formatString":"#,0.000",
        "units":"Index 1982-1984=100"
    },
    {
        "seriesName": "All Employees, Total Nonfarm",
        "series_id": "PAYEMS",
        "tableName": "NonfarmPayrolls",
        "seriesDisplayName": "Nonfarm Payrolls",
        "formatString":"#,0",
        "units":"Thousands of Persons"

    },
    {
        "seriesName": "Unemployment Rate",
        "series_id": "UNRATE",
        "tableName": "UnemploymentRate",
        "seriesDisplayName": "Unemployment Rate",
        "formatString":"0.0",
        "units":"Percent"

    },
    {
        "seriesName": "Average Hourly Earnings of All Employees, Total Private",
        "series_id": "CES0500000003",
        "tableName": "AverageHourlyEarnings",
        "seriesDisplayName": "Average Hourly Earnings",
        "formatString":"0.00",
        "units":"Dollars per Hour"
    },
    {
        "seriesName": "State Minimum Wage Rate for New York",
        "series_id": "STTMINWGNY",
        "tableName": "NYCMinimumWage",
        "seriesDisplayName": "NYC Minimum Wage",
        "formatString":"0.00",
        "units":"Dollars per Hour"
    },
    {
        "seriesName": "State Minimum Wage Rate for New Jersey",
        "series_id": "STTMINWGNJ",
        "tableName": "NJMinimumWage",
        "seriesDisplayName": "NJ Minimum Wage",
        "formatString":"0.00",
        "units":"Dollars per Hour"
    },
    {
        "seriesName": "University of Michigan: Consumer Sentiment",
        "series_id": "UMCSENT",
        "tableName": "ConsumerConfidenceIndex",
        "seriesDisplayName": "Consumer Confidence Index",
        "formatString":"0.0",
        "units":"Index 1966:Q1=100"
    },
    {
        "seriesName": "Advance Retail Sales: Retail Trade",
        "series_id": "RSXFS",
        "tableName": "RetailSalesControlGroup",
        "seriesDisplayName": "Retail Sales - Control Group",
        "formatString":"#,0",
        "units":"Millions of Dollars"
    },
    {
        "seriesName": "Average Hourly Earnings of All Employees, Leisure and Hospitality",
        "series_id": "CES7000000003",
        "tableName": "AverageHourlyEarningsOfAllEmployees",
        "seriesDisplayName": "Leisure & Hospitality Wages",
        "formatString":"0.00",
        "units":"Dollars per Hour"
    },
    {
        "seriesName": "Consumer Price Index for All Urban Consumers: Club Membership for Shopping Clubs, Fraternal, or Other Organizations, or Participant Sports Fees in U.S. City Average",
        "series_id": "CUSR0000SERF01",
        "tableName": "ConsumerPriceIndexForAllUrbanConsumers",
        "seriesDisplayName": "CPI for All Urban Consumers - Club Membership",
        "formatString":"0.00",
        "units":"Index Dec 1997=100"
    },
    {
        "seriesName": "Average Price: Electricity per Kilowatt-Hour in New York-Newark-Jersey City, NY-NJ-PA",
        "series_id": "APUS12A72610",
        "tableName": "AveragePriceElectricityPerKilowattHour",
        "seriesDisplayName": "Electricity Price (NY Metro)",
        "formatString":"0.000",
        "units":"U.S. Dollars"
    },
    {
        "seriesName": "Producer Price Index by Industry: Plumbing, Heating and Air-Conditioning Contractors, Nonresidential Building Work",
        "series_id": "PCU23822X23822X",
        "tableName": "ProducerPriceIndexByIndustry",
        "seriesDisplayName": "PPI — Contractors",
        "formatString":"0.000",
        "units":"Index Dec 2007=100"
    }
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function that**
# - Fetches API response 
# - Saves raw files into macro folder 
# - Loads tables into dbo schema 
# - Logs run details


# CELL ********************

def downloadData(series_id: str, seriesName: str, tableName: str):
    try:
        apiEndpoint = f"{api_endpoint_url}?series_id={series_id}&api_key={api_key}&file_type={file_type}"
        response = requests.get(apiEndpoint)

        if response.status_code == 200:
            rawDataJson = response.json()
            observations = rawDataJson.get("observations", [])

            if len(observations) == 0:
                print(f"No data found for {seriesName}")
                return
                
            file_path = f"{destination_files_folder}{seriesName}.json"
            notebookutils.fs.put(file_path, json.dumps(observations), overwrite=True)
            print(f"Saved raw API file: {file_path}")

            # Convert to Spark DataFrame
            df = spark.createDataFrame(pd.DataFrame(observations))

            # Getting each full table name in dbo schema
            fullTableName = f"{destination_schema}.{destination_table_prefix}{tableName}"

            # write data directly into dbo tables (overwrite mode)
            df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(fullTableName)
            print(f"Loaded data into table {fullTableName}")

            # Log entry (append mode)
            spark.createDataFrame([(
                seriesName,
                series_id,
                fullTableName,
                datetime.datetime.now(),
                df.count()
            )], schema=logTableSchema).write.mode("append").format("delta").saveAsTable(f"{destination_logging_table}")

        else:
            print(f"API error {response.status_code} for series {seriesName}")

    except Exception as e:
        print(f"Error while running for {seriesName}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Runs data extraction process

# CELL ********************

for document in requiredDocuments:
    downloadData(document["series_id"], document["seriesName"], document["tableName"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Silver layer

# MARKDOWN ********************

# Initializes Silver layer destinations

# CELL ********************

#Final Table in silver
silver_schema = "silver"
silver_table_name = destination_table_prefix + "macro_data"
silver_final_table = silver_schema + "." + silver_table_name

# List of bronze tables with their series_id
bronze_tables = [(doc["tableName"], doc["series_id"]) for doc in requiredDocuments]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Creates the schema for fred_macro_data

# CELL ********************

# Silver table schema
macroDataSchema = StructType([
    StructField("seriesId", StringType()),
    StructField("date", DateType()),
    StructField("value", DoubleType())
])

silver_df = spark.createDataFrame([],schema=macroDataSchema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### For loop that 
# - Extracts data from Bronze layer
# - Applies transformations
# - Loads into Silver Layer Table

# CELL ********************

for table, series_id in bronze_tables:
    df = spark.table(f"dbo.{destination_table_prefix}{table}") \
        .drop("realtime_start", "realtime_end") \
        .withColumn("date", col("date").cast(DateType())) \
        .withColumn("value", col("value").cast(DoubleType())) \
        .withColumn("seriesId", lit(series_id))
    
    df = df.select("seriesId", "date", "value")

    silver_df = silver_df.unionByName(df)

silver_df.write.format("delta").mode("overwrite").option("overwriteschema","true").saveAsTable(f"{silver_final_table}")

print(f"Data is loaded to Silver table: {silver_final_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.createDataFrame(pd.DataFrame(requiredDocuments)).write.format("delta").mode("overwrite").option("overwriteschema","true").saveAsTable(f"{silver_schema}.{destination_table_prefix}seriesNames")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
