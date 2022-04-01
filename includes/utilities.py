# Databricks notebook source
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from delta import DeltaTable
from datetime import datetime
from typing import List



# COMMAND ----------

def ingest_batch_raw(path: str):
    return spark.read.format("json").option("inferSchema", True)\
           .option("multiline", True).load(path)


def transform_raw(raw:DataFrame):
    raw = raw.withColumn("movie", explode(col("movie")))
    return (raw.withColumn("datasource", lit("movieProject.databricks.com"))
                   .withColumn("ingesttime", current_timestamp())
                   .withColumn("status", lit("new"))
                   .withColumn("p_ingestdate", current_timestamp().cast("date"))) 


def batch_writer(
    dataframe: DataFrame,
    partition_column: str, 
    exclude_columns : List = [], 
    mode: str = "append"):
    return (dataframe.drop(*exclude_columns).write.format("delta").mode(mode).partitionBy(partition_column))

 
def batch_writer_silver(
    dataframe: DataFrame,
    exclude_columns : List = [], 
    mode: str = "append"):
    return (dataframe.drop(*exclude_columns).write.format("delta").mode(mode))
    

def batch_reader_bronze(spark: SparkSession):
    return spark.read.table("movie_bronze").filter("status = 'new'")
  

def transform_bronze(bronze: DataFrame):
    return bronze.select("movie", 
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
  
   

def generate_clean_and_quarantine_dataframes(dataframe: DataFrame):
    return (dataframe.filter("runtime > = 0" and "budget >= 1000000"),
           dataframe.filter("budget < 1000000" or "runtime < 0")
           )
    

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
    ):
    
    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    silverAugmented = dataframe.withColumn("status", lit(status)).dropDuplicates()
    
    update_match = "bronze.movie = dataframe.movie"
    update = {"status": "dataframe.status"}
    (
        bronzeTable.alias("bronze")
        .merge(silverAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )
    return True


