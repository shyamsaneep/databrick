# Databricks notebook source
df=spark.read.json("dbfs:/mnt/shelladlstech/raw/demo/drivers.json")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

name_schema=StructType([StructField("driverId", IntegerType()),
                        StructField("driverRef", StringType()),
                        StructField("number", IntegerType()),
                        StructField("code", StringType()),
                        StructField("name",MapType(StringType()),
                        StructField("dob", DateType()),
                        StructField("nationality", StringType()),
                        StructField("url", StringType())
     
])

# COMMAND ----------

df=spark.read.schema(name_schema).json("dbfs:/mnt/shelladlstech/raw/demo/drivers.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df1=df.withColumn("forename",df.name.forename).withColumn("surname",df.name.surname).drop("name")
display(df1)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dfnew=df.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

display(dfnew)

# COMMAND ----------

dfdriver=dfnew.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(dfdriver)

# COMMAND ----------

dfdriver.write.parquet("dbfs:/mnt/shelladlstech/raw/output/driver")

# COMMAND ----------

