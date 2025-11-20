# Databricks notebook source
############################################################
################# Clean and Load to Silver #################
#   - Bring through IMDB Merged, Netflix Titles and Netflix Credits  
#   - Filter IMDB data from 2010 to 2025
#   - Standardise column names (match imdb to netflix)  
#   - Trim string columns
#   - Carry out data type conversions where appropriate
#   - Extract IMDB ids from URL
#   - Drop dupes
#   - Load into silver tables
############################################################




############################################################
# Imports, Functions and intilisations
############################################################

from pyspark.sql.functions import col, trim, regexp_replace, regexp_extract
from pyspark.sql.types import IntegerType, DoubleType

def trim_string_cols(df):
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, trim(col(name)))
    return df

def km_to_int(col_expr):
    tmp = regexp_replace(col_expr, "M$", "e+06")
    tmp = regexp_replace(tmp, "K$", "e+03")
    return tmp.cast("double").cast("int")

  
BRONZE = "bronze_catalog.movies"
SILVER = "silver_catalog.movies" 
############################################################
# Clean and Load IMDB Merged Data
############################################################

imdb_merged_bronze = spark.table(f"{BRONZE}.bronze_imdb_merged")

imdb_merged_silver = (
    imdb_merged_bronze
    .filter((col("fileYear") >= 2010) & (col("fileYear") <= 2025))
    .transform(trim_string_cols)
    .withColumn("year", col("year").cast(IntegerType()))
    .withColumnRenamed("duration", "runtime")
    .withColumnRenamed("mpa", "ageCertification")
    .withColumn("rating", col("rating").cast(DoubleType()))
    .withColumn("votes", km_to_int(col("votes")))
    .withColumn("imdbId", regexp_extract(col("movieLink"), r"/title/(tt[0-9]+)/", 1))
    .withColumnRenamed("grossworldwwide", "grossWorldwide")
    .withColumnRenamed("year", "releaseYear")
    .withColumnRenamed("rating", "imdbScore")
    .withColumnRenamed("votes", "imdbVotes")
    .dropDuplicates()
)

imdb_merged_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER}.silver_imdb_merged")

############################################################
# Clean and Load Netflix Titles Data
############################################################


netflix_titles_bronze = spark.table(f"{BRONZE}.bronze_netflix_titles")

netflix_titles_silver = (
    netflix_titles_bronze
    .transform(trim_string_cols)
    .withColumn("releaseYear", col("releaseYear").cast(IntegerType()))
    .withColumn("runtime", col("runtime").cast(IntegerType()))
    .withColumn("seasons", col("seasons").cast(IntegerType()))
    .withColumn("imdbScore", col("imdbScore").cast(DoubleType()))
    .withColumn("imdbVotes", col("imdbVotes").cast(IntegerType()))
    .withColumn("tmdbPopularity", col("tmdbPopularity").cast(DoubleType()))
    .withColumn("tmdbScore", col("tmdbScore").cast(IntegerType()))
    .dropDuplicates()
)

netflix_titles_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER}.silver_netflix_titles")

############################################################
# Clean and Load Netflix Credits Data
############################################################


netflix_credits_bronze = spark.table(f"{BRONZE}.bronze_netflix_credits")

netflix_credits_silver = (
    netflix_credits_bronze
    .transform(trim_string_cols)
    .dropDuplicates()
)

netflix_credits_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER}.silver_netflix_credits")