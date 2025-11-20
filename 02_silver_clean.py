# Databricks notebook source
# MAGIC %md
# MAGIC ## 02_silver_clean
# MAGIC - Bring through IMDB Merged, Netflix Titles and Netflix Credits  
# MAGIC - Filter IMDB data from 2010 to 2025
# MAGIC - Standardise column names (match imdb to netflix)  
# MAGIC - Trim string columns
# MAGIC - Carry out data type conversions where appropriate
# MAGIC - Extract IMDB ids from URL
# MAGIC - Drop dupes
# MAGIC - Load into silver tables
# MAGIC
# MAGIC ### Assumptions
# MAGIC - Merged imdb data is identical to combination of other 2 imdb files, and no need to bring them through to silver separately
# MAGIC - Although merged imdb data contains more data than is required for the exercise, there may be future requirements that would benefit from having the extra data loaded into silver - although more work would be required to clean some of the fields that are currently unused
# MAGIC - Highest number of votes is currently 3m, and is very unlikely to exceed int's threshold
# MAGIC - Normalisation (e.g. 3nf) is not required for this exercise, although would be helpful at this stage in a real and more complex enterprise model
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports, Functions and intilisations

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and Load IMDB Merged Data

# COMMAND ----------


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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and Load Netflix Titles Data

# COMMAND ----------


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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and Load Netflix Credits Data

# COMMAND ----------

netflix_credits_bronze = spark.table(f"{BRONZE}.bronze_netflix_credits")

netflix_credits_silver = (
    netflix_credits_bronze
    .transform(trim_string_cols)
    .dropDuplicates()
)

netflix_credits_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER}.silver_netflix_credits")