# Databricks notebook source
container_name       = "input"
storage_account_name = "shelladlstech"
client_id            = "f1b6ade3-6210-49c3-8286-d10a48b84571"
tenant_id            = "30cc3176-5a12-41ed-b15b-2e34c82f6e8c"
client_secret        = "l598Q~G6sbWWwryO1CJvyAr6Q28pr~eV6OU2lcoU"

# COMMAND ----------

container_name       = "input"
storage_account_name = "shelladlstech"
client_id            = dbutils.secrets.get(scope="keys", key="clientid")
tenant_id            = dbutils.secrets.get(scope="keys", key="tenantid")
client_secret        = dbutils.secrets.get(scope="keys", key="clientsecret")

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get(scope="keys", key="clientid")

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

# MAGIC %fs ls dbfs:/mnt/shelladlstech/input/

# COMMAND ----------

dbutils.fs.unmount("dbfs:/mnt/shelladlstech/input/")

# COMMAND ----------

