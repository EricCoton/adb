# Databricks notebook source
# MAGIC %md
# MAGIC #Normalize the movie table and create lookup tables

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configuration step

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ##Steps to get movie silver table

# COMMAND ----------

# MAGIC %run ./00_ingest_raw_data

# COMMAND ----------

# MAGIC %run ./01_raw_to_bronze

# COMMAND ----------

# MAGIC %run ./02_bronze_to_silver

# COMMAND ----------

# MAGIC %run ./03_silver_update

# COMMAND ----------

# MAGIC %md
# MAGIC ##Normalize moive table

# COMMAND ----------

dbutils.fs.rm(moviePath, True)

# COMMAND ----------

movieDF = spark.sql("""SELECT Id, Title, Budget, Runtime, Price, ReleaseDate, Overview, Revenue,Tagline, CreatedBy, CreatedDate,  BackdropUrl, ImdbUrl, PosterUrl, TmdbUrl, UpdatedBy, UpdatedDate, OriginalLanguage FROM movie_silver""").dropDuplicates()

display(movieDF)

# COMMAND ----------

movieDF.write.format("delta").mode("overwrite").save(moviePath)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_silver_final
"""
)

spark.sql(
    f"""
CREATE TABLE movie_silver_final
USING DELTA
LOCATION "{moviePath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_silver_final

# COMMAND ----------

# MAGIC %md
# MAGIC ##Make a genre lookup table

# COMMAND ----------

dbutils.fs.rm(genrePath, True)

# COMMAND ----------

genreFirstDF = spark.sql("select Id, genres from movie_silver").select(explode("genres").alias("genres"))
genreSecondDF = genreFirstDF.select("genres.id", "genres.name").dropna(how = "any").dropDuplicates().filter("name != ''").sort("id")
genreSecondDF.write.format('delta').mode("overwrite").save(genrePath)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS genre_silver
"""
)

spark.sql(
    f"""
CREATE TABLE genre_silver
USING DELTA
LOCATION "{genrePath}"
"""
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  genre_silver 

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Make a junction table to link movie table and genre table

# COMMAND ----------

movie_genreFDF = spark.sql("select id, genres from movie_silver").select(col("id").alias("MovieID"), explode('genres').alias("genre"))
movie_genreSDF = movie_genreFDF.select("MovieID", col("genre.id").alias("genreID"), col("genre.name").alias("genreName")).dropna(how = 'any').dropDuplicates()
movie_genreSDF.write.format("delta").mode("overwrite").save(movieGenrePath)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS movieGenre_silver")
spark.sql(f"""
          CREATE TABLE movieGenre_silver
          USING DELTA
          LOCATION "{movieGenrePath}"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movieGenre_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##Make a originalLanguage lookup table

# COMMAND ----------

dbutils.fs.rm(LanguagePath, True)

# COMMAND ----------

originalLanguageFDF = spark.sql("select originalLanguage from movie_silver_final").dropDuplicates()
originalLanguageSDF = originalLanguageFDF.withColumn("languageName", lit("english"))\
                                         .withColumnRenamed("originalLanguage", "LanguageKey")
display(originalLanguageSDF)



# COMMAND ----------

originalLanguageSDF.write.format('delta').mode("overwrite").save(LanguagePath)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS originalLanguage_silver
"""
)

spark.sql(
    f"""
CREATE TABLE originalLanguage_silver
USING DELTA
LOCATION "{LanguagePath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM originalLanguage_silver
