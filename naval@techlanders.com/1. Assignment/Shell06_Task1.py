# Databricks notebook source

dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/task/shell06",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":
                   "H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="}
)


# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

display(dbutils.fs.ls("/mnt/task/shell06/input"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the data from csv

# COMMAND ----------

superStoreSrcDF = spark.read.format("csv").option("header","true").load("/mnt/task/shell06/input/Superstore data.csv")

# COMMAND ----------

superStoreSrcDF.printSchema()

# COMMAND ----------

renamed_df = superStoreSrcDF.select([F.col(col).alias(col.replace(' ', '_').replace('/', '_')) for col in superStoreSrcDF.columns])
renamed_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### write the data in delta format

# COMMAND ----------

renamed_df.write.format("delta").mode("overwrite").save("/mnt/task/shell06/output/shell06/superstoredata")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Read data from delta table

# COMMAND ----------

deltaDF = spark.sql("""
                    SELECT 
                    Segment
                    ,SUM(Sales) As TotalSales
                    FROM 
                    delta.`/mnt/task/shell06/output/shell06/superstoredata`
                    GROUP BY Segment
                    """)

display(deltaDF)