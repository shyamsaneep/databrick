# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC - Step1: Load the Sample superstore data in ALDS Gen 2 in new container
# MAGIC - Step 2: Mount ADLS container to databricks using (Access Key or SAS or Service Principal)
# MAGIC - Step 3: Read CSV file from container and convert into external delta table (creating dataframe or Querying using SQL) 
# MAGIC - Step 4: Query Delta Table to get the grouped data of segments with sum of sales

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://task@sablobnly.blob.core.windows.net",
  mount_point = "/mnt/task",
  extra_configs = {"fs.azure.account.key.sablobnly.blob.core.windows.net":
                   "H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="}
)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/input/

# COMMAND ----------

display(dbutils.fs.ls("abfss://task@shelladlstech.dfs.core.windows.net/input/"))

# COMMAND ----------

dfShell28=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/task/input/input/Superstore data.csv")

# COMMAND ----------

# Create external table 
(
    dfShell28.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("path","/mnt/shelladlstech/raw/Shell28/task/taskOutput")
    .saveAsTable("task_Output_Shell28")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from task_Output_Shell28

# COMMAND ----------

Row ID,Order ID,Order Date,Ship Date,Ship Mode,Customer ID,Customer Name,Segment,Country/Region,City,State,Postal Code,Region,Product ID,Category,Sub-Category,Product Name,Sales,Quantity,Discount,Profit

# COMMAND ----------

Shell28Schema=StructType([StructField("Row ID",IntegerType()),
                         StructField("Order ID",IntegerType()),
                         StructField("Order Date",StringType()),
                         StructField("location",StringType()),
                         StructField("country",StringType()),
                         StructField("lat",DoubleType()),
                         StructField("lng",DoubleType()),
                         StructField("alt",IntegerType()),
                         StructField("url",StringType())
                        ])

# COMMAND ----------

(
    spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format","csv")
     .option("cloudFiles.inferColumnTypes","True")
     .option("cloudFiles.schemaEvolutionMode","rescue")
     .option("cloudFiles.schemaLocation","/mnt/shelladlstech/raw/Shell28/task/autoloader/schemaLocation")
     .load("/mnt/task/input/")
 .writeStream
     .option("checkpointLocation","/mnt/shelladlstech/raw/Shell28/task/autoloader") 
     .option("mergeSchema", "true")
     .table("Shell28_autoloader_table")
)

# COMMAND ----------

