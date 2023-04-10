# Databricks notebook source
df=spark.read.format('csv').option('header',True).option('inferschema',True).load('/mnt/shell40task/input/Superstore data.csv')

# COMMAND ----------

df.write.format('delta').mode('overwrite').option('path','/mnt/task/shell12/output/shell').saveAsTable('shell')

# COMMAND ----------

df.write.format('delta').mode('overwrite').option("delta.columnMapping.mode", "name").option('path','/mnt/task/shell12/output/shell22').saveAsTable('shell22')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from shell22

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended shell22

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select segment,sum(sales) as sales_amount 
# MAGIC  from shell22
# MAGIC  group by segment