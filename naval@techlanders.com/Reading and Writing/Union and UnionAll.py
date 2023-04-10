# Databricks notebook source
simpleData = [(1,"James","Sales","NY",90000,34,10000),
    (2,"Michael","Sales","NY",86000,56,20000),
    (3,"Robert","Sales","CA",81000,30,23000),
    (4,"Maria","Finance","CA",90000,24,23000)
             ]

# COMMAND ----------

schema=  ["id INT","employee_name STRING","department" "STRING","state" "STRING","salary" "long","age" "INT","bonus" "long"]

# COMMAND ----------

df = spark.createDataFrame(simpleData,schema)
df.printSchema()
display(df)

# COMMAND ----------


simpleData2 = [(1,"James","Sales","NY",90000,34,10000),
    (5,"Maria","Finance","CA",90000,24,23000),
    (3,"Jen","Finance","NY",79000,53,15000),
    (6,"Jeff","Marketing","CA",80000,25,18000),
    (7,"Kumar","Marketing","NY",91000,50,21000)]

df2 = spark.createDataFrame(simpleData2,schema)
display(df2)
df2.printSchema()

# COMMAND ----------

df_new=df.union(df2)

# COMMAND ----------

display(df_new)

# COMMAND ----------

df_new_unionall=df.unionAll(df2)

# COMMAND ----------

display(df_new_unionall)

# COMMAND ----------

