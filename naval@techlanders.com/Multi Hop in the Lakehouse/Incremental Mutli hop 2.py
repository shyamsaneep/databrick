# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Incremental Multi-Hop in the Lakehouse
# MAGIC 
# MAGIC Now that we have a better understanding of how to work with incremental data processing by combining Structured Streaming APIs and Spark SQL, we can explore the tight integration between Structured Streaming and Delta Lake.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe Bronze, Silver, and Gold tables
# MAGIC * Create a Delta Lake multi-hop pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Incremental Updates in the Lakehouse
# MAGIC 
# MAGIC Delta Lake allows users to easily combine streaming and batch workloads in a unified multi-hop pipeline. Each stage of the pipeline represents a state of our data valuable to driving core use cases within the business. Because all data and metadata lives in object storage in the cloud, multiple users and applications can access data in near-real time, allowing analysts to access the freshest data as it's being processed.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/sslh/multi-hop-simple.png)
# MAGIC 
# MAGIC - **Bronze** tables contain raw data ingested from various sources (JSON files, RDBMS data,  IoT data, to name a few examples).
# MAGIC 
# MAGIC - **Silver** tables provide a more refined view of our data. We can join fields from various bronze tables to enrich streaming records, or update account statuses based on recent activity.
# MAGIC 
# MAGIC - **Gold** tables provide business level aggregates often used for reporting and dashboarding. This would include aggregations such as daily active website users, weekly sales per store, or gross revenue per quarter by department. 
# MAGIC 
# MAGIC The end outputs are actionable insights, dashboards and reports of business metrics.
# MAGIC 
# MAGIC By considering our business logic at all steps of the ETL pipeline, we can ensure that storage and compute costs are optimized by reducing unnecessary duplication of data and limiting ad hoc querying against full historic data.
# MAGIC 
# MAGIC Each stage can be configured as a batch or streaming job, and ACID transactions ensure that we succeed or fail completely.

# COMMAND ----------

(spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format","csv")
         .option("cloudFiles.schemaLocation","/mnt/shelladlstech/raw/multihop/bronze/")
         .load("/mnt/shelladlstech/raw/streaminput/")
         .createOrReplaceTempView("raw_temp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW bronze_temp AS(SELECT *,current_timestamp() as current_time,input_file_name() as source_file from raw_temp )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_temp

# COMMAND ----------

(spark.table("bronze_temp")
        .writeStream
        .format("delta")
        .option("checkpointLocation","/mnt/shelladlstech/raw/multihop/bronze/checkpoint")
        .outputMode("append")
        .table("bronze")
)

# COMMAND ----------

(spark.readStream
       .table("bronze")
        .createOrReplaceTempView("silver_view"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW silver AS (SELECT Id, Name, Salary, Country from silver_view )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver

# COMMAND ----------

(spark.table("silver")
.writeStream
.format("delta")
 .option("checkpointLocation","/mnt/shelladlstech/raw/multihop/silver/checkpoint")
 .outputMode("append")
 .table("enriched_silver")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from enriched_silver

# COMMAND ----------

(spark.readStream
 .table("enriched_silver")
 .createOrReplaceTempView("gold_view")
 
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_view

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view gold_temp as (Select Country, sum(Salary) as TotalSalary from gold_view group by Country)

# COMMAND ----------

(spark.table("gold_temp")
 .writeStream
 .option("checkpointLocation","/mnt/shelladlstech/raw/multihop/gold/checkpoint")
 .outputMode("complete")
 .trigger(availableNow=True)
 .table("gold")
)

# COMMAND ----------

