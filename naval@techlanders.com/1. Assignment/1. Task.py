# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC - Step1: Load the Sample superstore data in ALDS Gen 2 in new container
# MAGIC - Step 2: Mount ADLS container to databricks using (Access Key or SAS or Service Principal)
# MAGIC - Step 3: Read CSV file from container and convert into external delta table (creating dataframe or Querying using SQL) 
# MAGIC - Step 4: Query Delta Table to get the grouped data of segments with sum of sales