# Databricks notebook source
# MAGIC %md
# MAGIC ###Define Data Path

# COMMAND ----------

userName = "eric_zhang"
landingPath = f"/mnt/{userName}/movieProject/"
rawPath = landingPath + "raw/"
bronzePath = landingPath + "bronze/"
silverPath = landingPath + "silver/"
silverQuarantinePath = landingPath + "silverQuarantinePath/"
moviePath = landingPath + "movie/"
genrePath = landingPath + "genre/"
LanguagePath = landingPath + "language/"
movieGenrePath = landingPath + "movieGenre/"


# COMMAND ----------

# MAGIC %md
# MAGIC Configure database 

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS movieProject_{userName}")
spark.sql(f"USE movieProject_{userName}")


# COMMAND ----------

# MAGIC %md
# MAGIC Import Utility Functions

# COMMAND ----------

# MAGIC %run ./utilities
