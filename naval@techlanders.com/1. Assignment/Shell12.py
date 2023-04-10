# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/task/shell12",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":
                   "H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="}
)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

display(dbutils.fs.ls("/mnt/task/shell12/input"))

# COMMAND ----------

superStoreSrcDF = spark.read.format("csv").option("header","true").load("/mnt/task/shell12/input/Superstore data.csv")

# COMMAND ----------

renamed_df = superStoreSrcDF.select([F.col(col).alias(col.replace(' ', '_').replace('/', '_')) for col in superStoreSrcDF.columns])
renamed_df.printSchema()

# COMMAND ----------

renamed_df.write.format("delta").mode("overwrite").save("/mnt/task/shell12/output/shell12/superstoredata")

# COMMAND ----------

deltaDF = spark.sql(""" SELECT Segment ,SUM(Sales) As TotalSales FROM 
                    delta.`/mnt/task/shell12/output/shell12/superstoredata`
                    GROUP BY Segment """)

display(deltaDF)