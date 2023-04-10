# Databricks notebook source
dfschema2=spark.read.csv("dbfs:/mnt/shelladlstech/raw/demo/circuits.csv")

# COMMAND ----------

display(dfschema2)

# COMMAND ----------

