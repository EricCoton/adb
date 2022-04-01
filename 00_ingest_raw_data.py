# Databricks notebook source
# MAGIC %md
# MAGIC ###Step of Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ###Make notebook idempotent

# COMMAND ----------

dbutils.fs.rm(landingPath, True)


# COMMAND ----------

# MAGIC %md
# MAGIC ##mount azure storage account to dbfs to have source data ready

# COMMAND ----------

# MAGIC %run ./includes/mount_azure_storage

# COMMAND ----------

# MAGIC %md
# MAGIC ##Display the raw data directory

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest raw data

# COMMAND ----------

filePath = [file.path for file in dbutils.fs.ls(rawPath)]
rawDF = ingest_batch_raw(filePath)
display(rawDF)
    
