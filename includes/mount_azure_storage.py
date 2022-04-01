# Databricks notebook source
# MAGIC %md
# MAGIC ###mount azure blob storage to databricks file storage system

# COMMAND ----------

# MAGIC %run ./configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ###using azure account key mount the azure account container to dbfs

# COMMAND ----------

sourcePath = "wasbs://raw@movieproject.blob.core.windows.net"
mount_point_path = landingPath + "raw/"
config_key = "fs.azure.account.key.movieproject.blob.core.windows.net"
blob_account_key = "xHrNIJmGzKva1Ie8UKW7bAeFPtTATtDVbiNiiDpE8o4SsS/M5XtSEH7jkCXGUn5zDIQutb/nk5gb+AStYm2wJw=="
dbutils.fs.unmount(mount_point_path)

dbutils.fs.mount(source = sourcePath,
                 mount_point = mount_point_path,  
                extra_configs = {config_key:blob_account_key})

# COMMAND ----------

dbutils.fs.ls(mount_point_path)
