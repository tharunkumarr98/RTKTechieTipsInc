# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6bff0765-0ffd-4c03-be0e-eb908c63053d",
# META       "default_lakehouse_name": "AdventureWorksDB",
# META       "default_lakehouse_workspace_id": "5bb7e532-b8dc-4144-986f-8d0e5112c4ee",
# META       "known_lakehouses": [
# META         {
# META           "id": "6bff0765-0ffd-4c03-be0e-eb908c63053d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd
from tqdm.auto import tqdm
base = "https://synapseaisolutionsa.z13.web.core.windows.net/data/AdventureWorks"

# load list of tables
df_tables = pd.read_csv(f"{base}/adventureworks.csv", names=["table"])

for table in (pbar := tqdm(df_tables['table'].values)):
    pbar.set_description(f"Uploading {table} to lakehouse")

    # download
    df = pd.read_parquet(f"{base}/{table}.parquet")

    # save as lakehouse table
    spark.createDataFrame(df).write.mode('overwrite').saveAsTable(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# - **Schema mismatch error cause:**  
#   The error occurred because the schema of the incoming DataFrame did not match the existing Delta table's schema, specifically for `CarrierTrackingNumber` and `CustomerPONumber`.
# 
# - **Void type issue:**  
#   The existing Delta table had `void` as the type for these columns, which is invalid and incompatible with the new DataFrame where these columns are `string`.
# 
# - **How missing columns occur in Spark:**  
#   Missing columns or null values often happen when loading data from schema-less sources like CSV or Parquet where not all rows have all columns, resulting in columns with many nulls but existing in schema.
# 
# - **Parquet file and Spark interaction:**  
#   Parquet files store schema union. Spark interprets missing or null values as `null`, but sometimes schema inference can lead to unexpected types or nullability issues.
# 
# - **Defining explicit schema:**  
#   To avoid inference issues, define an explicit schema using Spark StructTypes with correct types (replace `void` with `StringType` for nullable string columns).
# 
# - **Writing data with schema enforcement:**  
#   Use `.option("overwriteSchema", "true")` and `.mode("overwrite")` when writing the DataFrame to update the table schema.
# 
# - **Delta Lake schema evolution:**  
#   Delta Lake has strict schema enforcement; it stops write if incompatible schemas are detected. Use `.option("mergeSchema", "true")` to enable automatic schema evolution during writes if needed.
# 
# - **Resolving schema mismatch:**  
#   - Drop and recreate the Delta table with correct schema if possible.  
#   - Cast columns explicitly to the correct types before writing.  
#   - Use DeltaTable APIs for merge and schema evolution when appending/updating data.
# 
# - **Handling null/missing values:**  
#   Use `fillna()` to replace nulls with default values, or `dropna()` to remove rows missing critical column values.
# 
# - **Why table creation with missing values succeeds:**  
#   Spark allows nullable columns by default and accepts incomplete rows with nulls, so table creation does not fail on missing field values.


# CELL ********************

df_internetSales = pd.read_parquet(f"{base}/FactInternetSales.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, ShortType, StringType, DecimalType, DoubleType, TimestampType
spark.sql("DROP TABLE IF EXISTS AdventureWorksDB.factinternetsales")
schema = StructType([
    StructField("ProductKey", IntegerType(), True),
    StructField("OrderDateKey", IntegerType(), True),
    StructField("DueDateKey", IntegerType(), True),
    StructField("ShipDateKey", IntegerType(), True),
    StructField("CustomerKey", IntegerType(), True),
    StructField("PromotionKey", IntegerType(), True),
    StructField("CurrencyKey", IntegerType(), True),
    StructField("SalesTerritoryKey", IntegerType(), True),
    StructField("SalesOrderNumber", StringType(), True),
    StructField("SalesOrderLineNumber", ShortType(), True),
    StructField("RevisionNumber", ShortType(), True),
    StructField("OrderQuantity", ShortType(), True),
    StructField("UnitPrice", DecimalType(6, 2), True),
    StructField("ExtendedAmount", DecimalType(6, 2), True),
    StructField("UnitPriceDiscountPct", DoubleType(), True),
    StructField("DiscountAmount", DoubleType(), True),
    StructField("ProductStandardCost", DecimalType(6, 2), True),
    StructField("TotalProductCost", DecimalType(6, 2), True),
    StructField("SalesAmount", DecimalType(6, 2), True),
    StructField("TaxAmt", DecimalType(5, 2), True),
    StructField("Freight", DecimalType(4, 2), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("CustomerPONumber", StringType(), True),
    StructField("OrderDate", TimestampType(), True),
    StructField("DueDate", TimestampType(), True),
    StructField("ShipDate", TimestampType(), True)
])

spark.createDataFrame(df_internetSales,schema=schema).write.mode('overwrite').option("overwriteschema",  "true").saveAsTable("FactInternetSales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM AdventureWorksDB.factinternetsales LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 2025 Fact InternetSales

# CELL ********************

df = spark.sql("""
SELECT 
  ProductKey, 
  CAST(date_format(add_months(OrderDate, 140), 'yyyyMMdd') AS INT) as OrderDateKey,
  CAST(date_format(add_months(DueDate, 140), 'yyyyMMdd') AS INT) as DueDateKey,
  CAST(date_format(add_months(ShipDate, 140), 'yyyyMMdd') AS INT) as ShipDateKey,
  CustomerKey,
  PromotionKey,
  CurrencyKey, 
  SalesTerritoryKey, 
  SalesOrderNumber,
  SalesOrderLineNumber,
  RevisionNumber,
  OrderQuantity, 
  UnitPrice,
  ExtendedAmount,
  UnitPriceDiscountPct,
  DiscountAmount,
  ProductStandardCost,
  TotalProductCost,
  SalesAmount,
  TaxAmt,
  Freight,
  add_months(OrderDate, 140) as OrderDate,
  add_months(DueDate, 140) as DueDate,
  add_months(ShipDate, 140) as ShipDate
FROM AdventureWorksDB.factinternetsales where add_months(OrderDate, 140) <= current_date() order by OrderDate desc
""")
df.write.format("delta").mode("overwrite").saveAsTable("FactInternetSales2025")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Not required

# CELL ********************

df = spark.sql(""" 
SELECT  ProductKey,\
			CAST(date_format(add_months(OrderDate, 140), 'yyyyMMdd') AS INT) as OrderDateKey,
            CAST(date_format(add_months(DueDate, 140), 'yyyyMMdd') AS INT) as DueDateKey,
            CAST(date_format(add_months(ShipDate, 140), 'yyyyMMdd') AS INT) as ShipDateKey,
			ResellerKey,\
			EmployeeKey,\
			PromotionKey,\
			CurrencyKey,\
			SalesTerritoryKey,\
			SalesOrderNumber,\
			SalesOrderLineNumber,\
			RevisionNumber,\
			OrderQuantity,\
			UnitPrice,\
			ExtendedAmount,\
			UnitPriceDiscountPct,\
			DiscountAmount,\
			ProductStandardCost,\
			TotalProductCost,\
			SalesAmount,\
			TaxAmt,\
			Freight,\
			CarrierTrackingNumber,\
			CustomerPONumber,\
			add_months(OrderDate, 140) as OrderDate,
            add_months(DueDate, 140) as DueDate,
            add_months(ShipDate, 140) as ShipDate
FROM AdventureWorksDB.factresellersales
 where add_months(OrderDate, 140) <= current_date() order by OrderDate desc
""")
df.write.format("delta").mode("overwrite").saveAsTable("FactResellerSales2025")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
