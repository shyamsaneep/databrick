# Databricks notebook source
# MAGIC %md
# MAGIC 1. Access Key-- Deprecated 
# MAGIC 2. SAS Token-- Deprecated 
# MAGIC 3. Service Principal
# MAGIC 4. Unity Catalog

# COMMAND ----------

Cluster Authetication

# COMMAND ----------

name="Karan"
department="Data analyst"

# COMMAND ----------

print(f"I am {name} working as {department}")

# COMMAND ----------

container_name       = "raw"
storage_account_name = "sanlydemo123"
client_id            = "f4e1e7f8-650e-4a83-92d7-a77deacd90fb"
tenant_id            = "30cc3176-5a12-41ed-b15b-2e34c82f6e8c"
client_secret        = "pAN8Q~x8ggVReJZjNYYFZPx.s0j_NsREWVMDZcqw"

# COMMAND ----------

container_name       = "raw"
storage_account_name = "sanlydemo123"
client_id            = dbutils.secrets.get(scope="samplesecrets", key="clientid")
tenant_id            = dbutils.secrets.get(scope="samplesecrets", key="tenantid")
client_secret        = dbutils.secrets.get(scope="samplesecrets", key="clientsecret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sanlydemo123/raw/input/

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/mnt/sanlydemo123/raw/input/Jan.CSV")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get(scope="samplesecrets", key="clientid")

# COMMAND ----------

dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

# COMMAND ----------

