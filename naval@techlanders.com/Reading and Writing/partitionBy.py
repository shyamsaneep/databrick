# Databricks notebook source
# MAGIC %md 
# MAGIC ##### Partition By
# MAGIC - Writing Data by Partition By

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/mnt/shelladlstech/raw/demo/Baby_Names.csv")


# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy("Year").count().show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df.groupBy("Year").count().sort(col("Year")).show()

# COMMAND ----------

df.write.partitionBy("Year").parquet("dbfs:/mnt/shelladlstech/raw/output/partition/year")

# COMMAND ----------

df.write.format("parquet").partitionBy("Year","Sex").mode("overwrite").save("dbfs:/mnt/shelladlstech/raw/output/partition/year&gender")

# COMMAND ----------

df.write.format("parquet").option("maxRecordsPerFile",4000).partitionBy("Year").mode("overwrite").save("dbfs:/mnt/shelladlstech/raw/output/partition/year&maxfiles")

# COMMAND ----------

df.write.format("parquet").option("maxPartitionBytes",4000).partitionBy("Year").mode("overwrite").save("dbfs:/mnt/shelladlstech/raw/output/partition/year&maxfiles")