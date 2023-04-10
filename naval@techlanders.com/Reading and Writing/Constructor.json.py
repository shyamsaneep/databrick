# Databricks notebook source
df=spark.read.json("dbfs:/mnt/shelladlstech/raw/demo/constructor.json")

# COMMAND ----------

display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Dataframe functions
# MAGIC 1. Select
# MAGIC 2. alias
# MAGIC 3. withColumn
# MAGIC 4. withColumnRenamed
# MAGIC 5. filter & where
# MAGIC 6. sort & orderBy
# MAGIC 
# MAGIC #### Functions
# MAGIC 1. col
# MAGIC 2. lit
# MAGIC 3. concat
# MAGIC 4. current_timestamp()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df1=df.withColumnRenamed("constructorId","constructor_Id")

# COMMAND ----------

display(df1)

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df2=df1.withColumn("name",concat("name","constructorRef"))
display(df2)

# COMMAND ----------

df2=df1.withColumn("ingestion_date",current_timestamp())
display(df2)

# COMMAND ----------

df2.write.mode("overwrite").parquet("dbfs:/mnt/shelladlstech/raw/output/contructor")

# COMMAND ----------

dfcheck=spark.read.parquet("dbfs:/mnt/shelladlstech/raw/output/contructor")

# COMMAND ----------

display(dfcheck)

# COMMAND ----------

df2.write.mode("overwrite").delta("dbfs:/mnt/shelladlstech/raw/output/contructor_delta")

# COMMAND ----------

df2.write.mode("overwrite").format("delta").save("dbfs:/mnt/shelladlstech/raw/output/contructor_delta")

# COMMAND ----------

df2.write.format("parquet").saveAsTable("constructor_par")

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE Database shell18

# COMMAND ----------

df2.write.saveAsTable("constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table constructor

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table default.constructors_json

# COMMAND ----------

dfcheck=spark.read.format("delta").load("dbfs:/mnt/shelladlstech/raw/output/contructor_delta")

# COMMAND ----------

display(dfcheck)

# COMMAND ----------

