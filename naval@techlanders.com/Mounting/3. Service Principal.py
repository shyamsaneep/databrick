# Databricks notebook source
# MAGIC %md
# MAGIC #### Mounting ADLS Gen 2 using Service Principal

# COMMAND ----------

# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/getting-started/connect-to-azure-storage

# COMMAND ----------

container_name       = "raw"
storage_account_name = "shelladlstech"
client_id            = "25a1d5c9-bf36-4d79-965b-a7b9987a5760"
tenant_id            = "30cc3176-5a12-41ed-b15b-2e34c82f6e8c"
client_secret        = ".TL8Q~jttuX-jKjHlfFFt78u99urWSE_k_V53dab"

# COMMAND ----------

container_name       = ""
storage_account_name = ""
client_id            = ""
tenant_id            = ""
client_secret        = ""

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

# MAGIC %fs ls dbfs:/mnt/raw/

# COMMAND ----------

# MAGIC %md
# MAGIC ### F String

# COMMAND ----------

name= "Virat"
dept= "Data Engineer"

# COMMAND ----------

print(f"I am {name} working in {dept}")

# COMMAND ----------

# MAGIC %md
# MAGIC Secrets using Azure key vaults

# COMMAND ----------

storage_account_name = "shelladlstech"
client_id            = dbutils.secrets.get(scope="raw",key="clientid")
tenant_id            = dbutils.secrets.get(scope="raw",key="tenantid")
client_secret        = dbutils.secrets.get(scope="raw",key="clientsecret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get(scope="raw",key="clientid")

# COMMAND ----------

dbutils.secrets.list("raw")

# COMMAND ----------

for i in dbutils.secrets.get(scope="raw",key="clientid"):
    print(i)

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")