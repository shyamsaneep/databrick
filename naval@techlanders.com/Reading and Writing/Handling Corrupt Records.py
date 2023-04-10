# Databricks notebook source
# MAGIC %md
# MAGIC #### Handling Corrupt Records
# MAGIC - Permissive - Include Corrupt record in separate column
# MAGIC - Drop Malformed - Ignore Corrupt records
# MAGIC - Fail Fast- Throw Exception if corrupt record

# COMMAND ----------

# MAGIC %md
# MAGIC https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/mnt/shelladlstech/raw/demo/EmployeesTable_corrupt_records.CSV")

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/shelladlstech/raw/demo/EmployeesTable_corrupt_records2.CSV")
display(df)

# COMMAND ----------

display(df)

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/mnt/shelladlstech/raw/demo/EmployeesTable_corrupt_records.CSV")
display(df)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

user_schema=StructType([StructField("Id",IntegerType()),
                       StructField("Name",StringType()),
                        StructField("Gender",StringType()),
                        StructField("Salary",IntegerType()),
                        StructField("Country",StringType()),
                        StructField("Corrupt_record", StringType())
                       
                       ])

# COMMAND ----------

df=spark.read.option("header",True).schema(user_schema).csv("dbfs:/mnt/shelladlstech/raw/demo/EmployeesTable_corrupt_records.CSV")
display(df)

# COMMAND ----------

df=spark.read.option("header",True).option("mode","Permissive").schema(user_schema).csv("dbfs:/mnt/shelladlstech/raw/demo/EmployeesTable_corrupt_records.CSV")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC dropmalformed

# COMMAND ----------

df=spark.read.option("header",True).option("mode","dropmalformed").schema(user_schema).csv("dbfs:/mnt/shelladlstech/raw/demo/EmployeesTable_corrupt_records.CSV")
display(df)

# COMMAND ----------

df=spark.read.option("header",True).option("mode","failfast").schema(user_schema).csv("dbfs:/mnt/shelladlstech/raw/demo/EmployeesTable_corrupt_records.CSV")
display(df)

# COMMAND ----------

df=spark.read.option("header",True).schema(user_schema).csv("dbfs:/mnt/shelladlstech/raw/demo/EmployeesTable_corrupt_records.CSV")
display(df)

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/mnt/shelladlstech/raw/demo/EmployeesTable_corrupt_records.CSV")
display(df)

# COMMAND ----------

df.write.format("delta").save("dbfs:/mnt/shelladlstech/raw/output/employee")

# COMMAND ----------

dfemp=spark.read.format("delta").load("dbfs:/mnt/shelladlstech/raw/output/employee")
display(dfemp)

# COMMAND ----------

dfemp=spark.read.delta("dbfs:/mnt/shelladlstech/raw/output/employee")

# COMMAND ----------

