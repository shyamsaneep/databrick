# Databricks notebook source
# MAGIC %md
# MAGIC ##### DataFrame NaFunctions
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html?highlight=na%20functions#pyspark.sql.DataFrameNaFunctions

# COMMAND ----------

# MAGIC %md
# MAGIC - drop 
# MAGIC - fill
# MAGIC - replace 

# COMMAND ----------

data = [("James", "Sales", 3000), \
        (None, None, None),\
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", None), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    (None, "Finance", None), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("", None, 2000), \
    ("Saif", "Sales", None), \
    ("Maria", "Finance", 3000),\
    ("Maria", "Marketing", 3000)
  ]
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.dropna())

# COMMAND ----------

display(df.na.drop(how="any"))

# COMMAND ----------

display(df.na.drop("all"))

# COMMAND ----------

display(df.dropna(subset="employee_name"))

# COMMAND ----------

display(df.dropna(how="all", subset="employee_name"))

# COMMAND ----------

help(df.na.drop)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.dropna(thresh=3))

# COMMAND ----------

help(df.dropna)

# COMMAND ----------

help(df.dropDuplicates)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.drop_duplicates())

# COMMAND ----------

display(df.drop_duplicates(["employee_name"]))

# COMMAND ----------

display(df.distinct())

# COMMAND ----------

display(df.distinct(["employee_name"]))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

display(df.select(col("employee_name")).distinct())

# COMMAND ----------

data2 = [("James", "Sales", 3000), \
        (None, None, None),\
         (None, None, None),\
         (None, None, None),\
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", None), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    (None, "Finance", None), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("", None, 2000), \
    ("Saif", "Sales", None), \
    ("Maria", "Finance", 3000),\
    ("Maria", "Marketing", 3000)
  ]
columns2= ["employee_name", "department", "salary"]
df2 = spark.createDataFrame(data = data2, schema = columns2)
df2.printSchema()

# COMMAND ----------

display(df2)

# COMMAND ----------

display(df2.drop_duplicates())

# COMMAND ----------

help(df.fillna)

# COMMAND ----------

display(df.na.fill(1000))

# COMMAND ----------

display(df.na.fill(value="newcomer"))

# COMMAND ----------

display(df.fillna(value="newcomer", subset="employee_name"))

# COMMAND ----------

display(df.fillna({"employee_name":"newcomer","department":"IT","salary":1000}))

# COMMAND ----------

Try to replace the empty value with John using replace function

# COMMAND ----------

display(df.fillna(True))

# COMMAND ----------

help(df.regexp_replace())

# COMMAND ----------

df.replace("",'tq').display()