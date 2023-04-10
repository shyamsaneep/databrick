# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.combobox(name="box",defaultValue="Apple",choices=["Apple","Samsung", "MI","Nokia"],label="Mobiles")

# COMMAND ----------

dbutils.widgets.get("box")

# COMMAND ----------

dbutils.widgets.dropdown("cars","Tata",choices=["Tata","Kia", "Maruti","Honda"],label="CarComapanies")

# COMMAND ----------

dbutils.widgets.get("cars")

# COMMAND ----------

