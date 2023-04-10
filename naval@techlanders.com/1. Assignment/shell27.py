# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 - Mounting using access key

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/shell27",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

# MAGIC %fs ls /mnt/shell27

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/shell27/input/

# COMMAND ----------



# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/shell27/input/Superstore data.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

shell27_schema=StructType([StructField("Row ID",IntegerType()),
                         StructField("Order ID",StringType()),
                         StructField("Order Date",StringType()),
                         StructField("Ship Date",StringType()),
                         StructField("Ship Mode",StringType()),
                         StructField("Customer ID",DoubleType()),
                         StructField("Customer Name",DoubleType()),
                         StructField("",IntegerType()),
                         StructField("",StringType()),
                         StructField("Ship Mode",StringType()),
                         StructField("Ship Mode",StringType()),
                         StructField("Ship Mode",StringType()),
                         StructField("Ship Mode",StringType()),
                         StructField("Ship Mode",StringType())
                        ])