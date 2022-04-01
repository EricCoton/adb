# Databricks notebook source
# MAGIC %md
# MAGIC ###bronze to silver 

# COMMAND ----------

# MAGIC %md
# MAGIC #Configuration step

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC #Display the files in bronze path

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC #Make notebook idempotent

# COMMAND ----------

dbutils.fs.rm(bronzePath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Current Delta Architecture

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Step 1: Create the rawDF DataFrame

# COMMAND ----------

filePath = [file.path for file in dbutils.fs.ls(rawPath)]
rawDF = ingest_batch_raw(filePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 2: Transform the raw data

# COMMAND ----------

transformedDF = transform_raw(rawDF).select("datasource", "ingesttime", "status", "movie", "p_ingestdate")
display(transformedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the Schema wih an Assertion

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.types import * 
# MAGIC assert transformedDF.schema == StructType(
# MAGIC     [   StructField("datasource", StringType(), False),
# MAGIC         StructField("ingesttime", TimestampType(), False),
# MAGIC         StructField("status", StringType(), False),
# MAGIC         StructField("movie", StringType(), True),
# MAGIC         StructField("p_ingestdate", DateType(), False)
# MAGIC     ]
# MAGIC )
# MAGIC print("Assertion passed.")
# MAGIC 
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 3 : Write batch to a bronze table

# COMMAND ----------

rawToBronzeWriter = batch_writer(dataframe = transformedDF, partition_column = "p_ingestdate")
rawToBronzeWriter.save(bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC #Display the bronze table

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC #Bronze to silver step

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Make notebook idempotent

# COMMAND ----------

dbutils.fs.rm(silverPath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load new record from the bronze records

# COMMAND ----------

bronzeDF = spark.read.table("movie_bronze").filter("status = 'new'")
display(bronzeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #Extract the nested JSON from teh bronze records

# COMMAND ----------

bronzeAugmentedDF = bronzeDF.select("movie", 
                                    "movie.BackdropUrl", 
                                    col("movie.Budget").cast("double"),
                                    "movie.CreatedBy", 
                                    col("movie.CreatedDate").cast("date"),
                                    "movie.id",
                                    "movie.imdbUrl", 
                                    "movie.OriginalLanguage", 
                                    "movie.Overview",
                                    "movie.PosterUrl",
                                    "movie.Price",
                                    col("movie.ReleaseDate").cast("date"),
                                    "movie.Revenue", 
                                    "movie.RunTime", 
                                    "movie.Tagline", 
                                    "movie.TmdbUrl", 
                                    "movie.Title", 
                                    "movie.UpdatedBy",
                                    col("movie.UpdatedDate").cast("date"),
                                    "movie.genres"
                                    )
display(bronzeAugmentedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##verify the Schema with an assertion

# COMMAND ----------

# MAGIC %md 
# MAGIC from pyspark.sql.types import _parse_datatype_string
# MAGIC assert bronzeAugmentedDF.schema == _parse_datatype_string(
# MAGIC     """
# MAGIC       movie STRING,
# MAGIC       BackdropUrl string,
# MAGIC       Budget double,
# MAGIC       CreatedBy string,
# MAGIC       CreatedDate DATE,
# MAGIC       Id long,
# MAGIC       ImdbUrl string,
# MAGIC       OriginalLanguage string,
# MAGIC       Overview string,
# MAGIC       PosterUrl string,
# MAGIC       Price double,
# MAGIC       ReleaseDate string,
# MAGIC       Revenue double,
# MAGIC       RunTime long,
# MAGIC       Tagline string,
# MAGIC       Title string,
# MAGIC       TmdbUrl string,
# MAGIC       UpdatedBy string,
# MAGIC       UpdatedDate DATE,
# MAGIC       genres array
# MAGIC       """), "Schemas do not match"
# MAGIC print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC #Quarantine the bad data

# COMMAND ----------

bronzeAugmentedDF.count()

# COMMAND ----------

bronzeAugmentedDF.na.drop(how = "all").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##splitthe silver DataFrame

# COMMAND ----------

silver_movie_clean = bronzeAugmentedDF.filter("RunTime > 0").filter("budget>=1000000")
silver_movie_quarantine= bronzeAugmentedDF.filter("RunTime <= 0 or budget <1000000")

# COMMAND ----------

# MAGIC %md
# MAGIC #Display the clean dataframe

# COMMAND ----------

display(silver_movie_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC #Display the quarantined data

# COMMAND ----------

display(silver_movie_quarantine)

# COMMAND ----------

silver_movie_clean.count()

# COMMAND ----------

silver_movie_quarantine.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #Write clean batch to a silver table

# COMMAND ----------

batch_writer_silver(silver_movie_clean, ["movie"]).save(silverPath)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS movie_silver")


spark.sql(f"""
          CREATE TABLE movie_silver
          USING DELTA
          LOCATION "{silverPath}"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC #Verify Schema with an assertion

# COMMAND ----------

# MAGIC %md
# MAGIC '''silverTable = spark.read.table("movie_silver")
# MAGIC expected_schema = """
# MAGIC       BackdropUrl string,
# MAGIC       Budget double,
# MAGIC       CreatedBy string,
# MAGIC       CreatedDate DATE,
# MAGIC       Id long,
# MAGIC       ImdbUrl string,
# MAGIC       OriginalLanguage string,
# MAGIC       Overview string,
# MAGIC       PosterUrl string,
# MAGIC       Price double,
# MAGIC       ReleaseDate string,
# MAGIC       Revenue double,
# MAGIC       RunTime long,
# MAGIC       Tagline string,
# MAGIC       Title string,
# MAGIC       TmdbUrl string,
# MAGIC       UpdatedBy string,
# MAGIC       UpdatedDate DATE,
# MAGIC       genres array
# MAGIC """
# MAGIC 
# MAGIC assert silverTable.schema == _parse_datatype_string(expected_schema), "Schemas do not match"
# MAGIC print("Assertion passed.")'''

# COMMAND ----------

# MAGIC %md
# MAGIC #Update Bronze table to reflect the loads

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1 : Update clean records

# COMMAND ----------

from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silver_movie_clean.withColumn("status", lit("Loaded")).dropDuplicates()

update_match = "bronze.movie = clean.movie"
update = {"status": "clean.status"}

( 
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 2: Update quarantined records

# COMMAND ----------

silverAugmented = silver_movie_quarantine.withColumn("status", lit("quarantine")).dropDuplicates()

update_match = "bronze.movie = quarantine.movie"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC  DESCRIBE TABLE EXTENDED movie_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from movie_silver
