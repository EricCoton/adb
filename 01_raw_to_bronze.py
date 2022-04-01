# Databricks notebook source
# MAGIC %md
# MAGIC Make raw data to bronze 

# COMMAND ----------

# MAGIC %md
# MAGIC #Configuration step

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC #Display files in the raw path

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC #Make notebook idempotent

# COMMAND ----------

dbutils.fs.rm(bronzePath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #ingest raw data

# COMMAND ----------

filePath = [file.path for file in dbutils.fs.ls(rawPath)]
rawDF = ingest_batch_raw(filePath)

# COMMAND ----------

# MAGIC %md
# MAGIC #Display raw data

# COMMAND ----------

display(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #Ingest Metadata

# COMMAND ----------

transformedDF = transform_raw(rawDF)
display(transformedDF)

# COMMAND ----------

# MAGIC %md 
# MAGIC #write batch to bronze table

# COMMAND ----------

batch_writer(transformedDF, "p_ingestdate").save(bronzePath)


# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC #Register the bronze table in the Metastore

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS movie_bronze")

spark.sql(f"""
          CREATE TABLE movie_bronze
          USING DELTA
          LOCATION "{bronzePath}"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #display the bronze table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from movie_bronze
