# Databricks notebook source
# MAGIC %md
# MAGIC #Silver table updates

# COMMAND ----------

# MAGIC %md
# MAGIC #Step configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

dbutils.fs.rm(bronzePath, True)
dbutils.fs.rm(silverPath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Current Delta Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC ##Raw to bronze pipeline

# COMMAND ----------

filePath = [file.path for file in dbutils.fs.ls(rawPath)]
rawDF = ingest_batch_raw(filePath)

transformedDF = transform_raw(rawDF).select("datasource", "ingesttime", "status", "movie", "p_ingestdate")
rawToBronzeWriter = batch_writer(dataframe = transformedDF, partition_column = "p_ingestdate")
rawToBronzeWriter.save(bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ##bronze to silver pipeline

# COMMAND ----------

bronzeDF = batch_reader_bronze(spark)
bronzeAugmentedDF = transform_bronze(bronzeDF)

(silver_movie_clean, silver_movie_quarantine )= generate_clean_and_quarantine_dataframes(bronzeAugmentedDF)

batch_writer_silver(silver_movie_clean, ["movie"]).save(silverPath)

update_bronze_table_status(spark, bronzePath, silver_movie_clean, "loaded")
update_bronze_table_status(spark, bronzePath, silver_movie_quarantine, "quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Perform a Visual Verification of the Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Quarantined Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Load Quarantined Records from the Bronze Table

# COMMAND ----------

bronzeQuarantinedDF = spark.read.table("movie_bronze").filter("status = 'quarantine'")
display(bronzeQuarantinedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Transform the Quarantined Records

# COMMAND ----------

bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF)
 
display(bronzeQuarTransDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 3 : fix the quarantined data

# COMMAND ----------

from pyspark.sql.functions import *

silver_quarantine_clean = bronzeQuarTransDF.withColumn("Budget", when(col("Budget") <=1000000, 1000000).when(col("Budget") >= 1000000, "Budget").otherwise(1000000).cast('double'))
display(silver_quarantine_clean)

silver_quarantine_clean = silver_quarantine_clean.withColumn("RunTime", abs("RunTime"))
display(silver_quarantine_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 : Batch Write the Repaired (formerly Quarantined) Records to the Silver Table

# COMMAND ----------

display(silver_quarantine_clean)

# COMMAND ----------

bronzeToSilverWriter = batch_writer_silver(
    dataframe = silver_quarantine_clean, exclude_columns=["movie"]
)
bronzeToSilverWriter.save(silverPath)

update_bronze_table_status(spark, bronzePath, silver_quarantine_clean, "Loaded")



# COMMAND ----------

# MAGIC %md
# MAGIC ##Display quarantined records and check the bronze table for results

# COMMAND ----------

display(silver_quarantine_clean)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from movie_bronze where status = 'quarantine'
