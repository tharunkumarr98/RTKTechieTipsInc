# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ea2bc855-4f8c-43e7-8426-e621aeeefdcd",
# META       "default_lakehouse_name": "employeeWorkingHours",
# META       "default_lakehouse_workspace_id": "53d4189d-08b9-4d9b-aed4-df92fed3841d",
# META       "known_lakehouses": [
# META         {
# META           "id": "ea2bc855-4f8c-43e7-8426-e621aeeefdcd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE employeeWorkingHours

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import IntegerType, StringType, FloatType, StructType, StructField, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
       StructField("EmployeeId", StringType(), True),
       StructField("EmployeeName", StringType(), True),
       StructField("Date", DateType(), True),
       StructField("WorkingHours", FloatType(), True)
   ])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").schema(schema).load("abfss://EmployeeWorkingHours@onelake.dfs.fabric.microsoft.com/employeeWorkingHours.Lakehouse/Files/employeeWorkingHours/WorkingHours_June8_June15_2025.csv")
df.write.format("delta").mode("overwrite").saveAsTable("employeeWorkingHours")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM employeeWorkingHours.dbo.employeeworkinghours LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
