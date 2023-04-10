# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1: Read the data and creating DF

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

users_schema=StructType([StructField("circuitId",IntegerType()),
                         StructField("circuitRef",StringType()),
                         StructField("name",StringType()),
                         StructField("location",StringType()),
                         StructField("country",StringType()),
                         StructField("lat",DoubleType()),
                         StructField("lng",DoubleType()),
                         StructField("alt",IntegerType()),
                         StructField("url",StringType())
                        ])

# COMMAND ----------

dfschema=spark.read.option("header",True).schema(users_schema).csv("dbfs:/mnt/shelladlstech/raw/demo/circuits.csv")



# COMMAND ----------

users_schema2=StructType([StructField("circuitId",IntegerType(),False),
                         StructField("circuitRef",StringType(),False),
                         StructField("name",StringType(),False),
                         StructField("location",StringType(),False),
                         StructField("country",StringType(),False),
                         StructField("lat",DoubleType(),False),
                         StructField("lng",DoubleType(),False),
                         StructField("altitude",StringType(),False),
                         StructField("url",StringType(),False)
                        ])
dfschema2=spark.read.option("header",True).schema(users_schema2).csv("dbfs:/mnt/shelladlstech/raw/demo/circuits.csv")
display(dfschema2)
dfschema2.printSchema()

# COMMAND ----------

df=spark.read.option("header",True).schema(users_schema2).csv("dbfs:/mnt/shelladlstech/raw/demo/circuits.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.cache()

# COMMAND ----------

df1=df.select("*")
display(df1)

# COMMAND ----------

df2=

# COMMAND ----------

display(dfschema2)
dfschema2.printSchema()

# COMMAND ----------

ferschemadf=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/shelladlstech/raw/demo/circuits.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(dfschema2)

# COMMAND ----------

help(col)

# COMMAND ----------

help(dfschema2.select)

# COMMAND ----------

display(dfschema2.select("*"))

# COMMAND ----------

dfschema2.select("circuitId","name").show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dfschema2.select(col("circuitId"),col("name")).show()

# COMMAND ----------

  dfschema2.select(dfschema2.circuitId,col("NAME"),"LocatioN",dfschema2["country"]).show()

# COMMAND ----------

dfschema2.select("name".alias("first_name"))

# COMMAND ----------

dfschema2.select(col("name").alias("first_name")).show()

# COMMAND ----------

display(dfschema2)

# COMMAND ----------

help(concat)

# COMMAND ----------

display(dfschema2.select("*", concat("location"," ", "country").alias("loc&county")))

# COMMAND ----------

help(lit)

# COMMAND ----------

display(dfschema2.select("*", concat("location",lit(" & "), "country").alias("loc&county")))

# COMMAND ----------

