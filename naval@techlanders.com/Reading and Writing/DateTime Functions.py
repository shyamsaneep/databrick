# Databricks notebook source
# MAGIC %md
# MAGIC #### Date Time Functions in PySpark  
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions

# COMMAND ----------

emp = [(1, "AAA", "dept1", 1000, "2019-02-01 15:12:13"),
    (2, "BBB", "dept1", 1100, "2018-04-01 5:12:3"),
    (3, "CCC", "dept1", 3000, "2017-06-05 1:2:13"),
    (4, "DDD", "dept1", 1500, "2019-08-10 10:52:53"),
    (5, "EEE", "dept2", 8000, "2016-01-11 5:52:43"),
    (6, "FFF", "dept2", 7200, "2015-04-14 19:32:33"),
    (7, "GGG", "dept3", 7100, "2019-02-21 15:42:43"),
    (8, "HHH", "dept3", 3700, "2016-09-25 15:32:33"),
    (9, "III", "dept3", 4500, "2017-10-15 15:22:23"),
    (10, "JJJ", "dept5", 3400, "2018-12-17 15:14:17")]
empdf = spark.createDataFrame(emp, ["id", "name", "dept", "salary", "date"])
display(empdf)
empdf.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### add_months

# COMMAND ----------



# COMMAND ----------

empdf.select("date").withColumn("next_month", add_months("date",1)).display()

# COMMAND ----------

empdf.withColumn("next_day",date_add("date", 1)).display()

# COMMAND ----------

empdf.withColumn("sub_date",date_sub("date", 1)).display()

# COMMAND ----------

empdf.withColumn("current_time",current_timestamp()).display()

# COMMAND ----------

df1= empdf.withColumn("new_date",date_format("date","dd.MM.yyyy"))
display(df1)
df1.printSchema()

# COMMAND ----------

empdf.withColumn("new_date",date_format("date","dd-MMM-yyyy")).display()

# COMMAND ----------

df2=empdf.withColumn("new_date",to_date("date"))
display(df2)
df2.printSchema()

# COMMAND ----------

df_new=df2.withColumn("new_date2",to_date("new_date","dd.MM.yyyy"))
display(df_new)
df_new.printSchema()

# COMMAND ----------

df2=empdf.withColumn("new_date",to_date(col("date"),"yyyy-MM-dd HH:mm:ss"))
display(df2)
df2.printSchema()

# COMMAND ----------

df2=empdf.select("date",to_date("date","yyyy-MM-dd HH:mm:ss"))
display(df2)
df2.printSchema()

# COMMAND ----------

empdf.select(to_date(empdf.date, 'yyyy-MM-dd HH:mm:ss').alias('date')).display()

# COMMAND ----------

df3=empdf.select("*").filter("id==1")

# COMMAND ----------

display(df3)

# COMMAND ----------

df4=df3.select("date",to_date(col("date"),"yyyy-MM-dd HH:mm:ss")).withColumn("new", to_date("date", to_timestamp()))
display(df4)
df4.printSchema()

# COMMAND ----------

display(empdf)

# COMMAND ----------

empdf.withColumn("newDate", date_trunc("year","date")).display()

# COMMAND ----------

empdf.withColumn("newDate", date_trunc("month","date")).display()

# COMMAND ----------

empdf.withColumn("newDate", date_trunc("hour","date")).display()

# COMMAND ----------

empdf.\
withColumn("current_date",current_date()).\
withColumn("diff", datediff("current_date","date")).\
display()

# COMMAND ----------

empdf.\
withColumn("dayofweek", dayofweek("date")).\
display()

# COMMAND ----------

