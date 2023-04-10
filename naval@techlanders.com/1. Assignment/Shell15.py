# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/Shell15",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":
                   "H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/mnt/Shell15/input/Superstore data.csv")
df.display()
df.printSchema()


# COMMAND ----------

df.columns

# COMMAND ----------

sample_schema= StructType([StructField("Row_ID",IntegerType()),
                           StructField("Order_ID",StringType()),
                           StructField("Order_Date",StringType()),
                           StructField("Ship_Date",StringType()),
                           StructField("Ship_Mode",StringType()),
                           StructField("Customer_ID",StringType()),
                           StructField("Customer_Name",StringType()),
                           StructField("Segment",StringType()),
                           StructField("Country",StringType()),
                           StructField("City",StringType()),
                           StructField("State",StringType()),
                           StructField("Postal_Code",IntegerType()),
                           StructField("Region",StringType()),
                           StructField("Product_ID",StringType()),
                           StructField("Sales",DoubleType()),
                           StructField("Quantity",IntegerType()),
                           StructField("Discount",DoubleType()),
                           StructField("Profit",DoubleType()),
                       
])

# COMMAND ----------

dfschema=spark.read.option("header",True).schema(sample_schema).csv("dbfs:/mnt/Shell15/input/Superstore data.csv")
dfschema.display()
dfschema.printSchema()


# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/Shell15/output/

# COMMAND ----------

dbfs:/mnt/Shell15/output/

# COMMAND ----------

dfschema.write.option("path","dbfs:/mnt/Shell15/output/shell15_task1/deltatable").saveAsTable("taskshell15")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taskshell15

# COMMAND ----------

# MAGIC %sql
# MAGIC select segment, sum(sales) from taskshell15 group by segment 

# COMMAND ----------

