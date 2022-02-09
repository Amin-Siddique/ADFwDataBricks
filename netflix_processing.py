# Databricks notebook source
# MAGIC %md #### Mount the Storage DataLake Gen2

# COMMAND ----------

# MAGIC %run "/dcad/parm"

# COMMAND ----------

#dbutils.notebook.run("parm",300)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": access_client_id,
           "fs.azure.account.oauth2.client.secret": client_secret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + directory_id + "/oauth2/token" }

# COMMAND ----------

#dbutils.fs.unmount("/mnt/datalakefordev/netflix/input/ADFwDataBricks/main/")

# COMMAND ----------

#%fs ls /mnt/datalakefordev/netflix/input/ADFwDataBricks/blob/main/input/ADFwDataBricks/main/

# COMMAND ----------

try:
    dbutils.fs.ls("/mnt/datalakefordev/netflix/input/ADFwDataBricks/blob/main/input/ADFwDataBricks/main/")
except Exception as e:
    print("Path is not mounted")

# COMMAND ----------

try:
    dbutils.fs.mount(
    source = "abfss://" + folder + "@" + storage_name + ".dfs.core.windows.net/",
    mount_point = "/mnt/datalakefordev/netflix/input/ADFwDataBricks/blob/main/input/ADFwDataBricks/main/",
    extra_configs = configs)
except Exception as e:
    print("Already Mounted")

# COMMAND ----------

source_loc = "/mnt/datalakefordev/netflix/input/ADFwDataBricks/blob/main/input/ADFwDataBricks/main/netflix.csv"

# COMMAND ----------

source_loc

# COMMAND ----------

dbutils.fs.ls("/mnt/datalakefordev/netflix/input/ADFwDataBricks/blob/main/input/ADFwDataBricks/main/")

# COMMAND ----------

netflix = spark.read.format("csv").option("header",True).load("/mnt/datalakefordev/netflix/input/ADFwDataBricks/blob/main/input/ADFwDataBricks/main/input/ADFwDataBricks/main/netflix.csv")

# COMMAND ----------

display(netflix)

# COMMAND ----------

netflix_tf = netflix.withColumnRenamed("release year","release_year").withColumnRenamed("user rating score","userrating")

# COMMAND ----------

netflix_view = netflix_tf.createOrReplaceTempView("netflix")

# COMMAND ----------

netflix_final = spark.sql("select distinct * from netflix where (release_year,userrating) in (select distinct release_year,max(userrating) as userrating from netflix where userrating is not null group by release_year)")

# COMMAND ----------

netflix_final.write.partitionBy("release_year").csv("/mnt/datalakefordev/netflix/input/ADFwDataBricks/blob/main/input/ADFwDataBricks/main/output")
