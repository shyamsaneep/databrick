# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/shell37",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":
                   "H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="}
)

# COMMAND ----------

df_shell37 = spark.read.option("header",True).option("inferschema",True).csv('/mnt/shell37/input/Superstore data.csv')

# COMMAND ----------

df_shell37.display()

# COMMAND ----------

df_shell37.printSchema()

# COMMAND ----------


df_shell37 = df_shell37.withColumnRenamed("Row ID","ROW_ID")\
                       .withColumnRenamed("Order ID","ORDER_ID")\
                       .withColumnRenamed("Order Date","ORDER_DATE")\
                       .withColumnRenamed("Ship Date","SHIP_DATE")\
                       .withColumnRenamed("Ship Mode","SHIP_MODE")\
                       .withColumnRenamed("Customer Name","CUSTOMER_NAME")\
                       .withColumnRenamed("Customer ID","CUSTOMER_ID")\
                       .withColumnRenamed("Country/Region","COUNTRY_REGION")\
                       .withColumnRenamed("Product ID","PRODUCT_ID")\
                       .withColumnRenamed("Sub-Category","SUB_CATEGORY")\
                       .withColumnRenamed("Product Name","PRODUCT_NAME")


# COMMAND ----------

df_shell37.printSchema()

# COMMAND ----------

df_shell37.display()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

#df_shell37strcttype = StructType [(StructField("ROW_ID",IntegerType),                                  StructField("ORDER_ID",IntegerType),]

# COMMAND ----------

df_shell37.write.format('delta').mode('overwrite').option("delta.columnMapping.mode","name").option('path','/mnt/shell37/output/shell37').saveAsTable('shell37_Superstore')