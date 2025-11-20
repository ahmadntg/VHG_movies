# Databricks notebook source
############################################################
################# 03 Load to Gold #########################
#   
############################################################

############################################################
# Imports, Functions and intilisations
############################################################
from pyspark.sql.functions import col, lower, trim, when, coalesce, row_number, min as spark_min
from pyspark.sql.window import Window

SILVER = "silver_catalog.movies"
GOLD   = "gold_catalog.movies"

netflix_titles  = spark.table(f"{SILVER}.silver_netflix_titles")
netflix_credits = spark.table(f"{SILVER}.silver_netflix_credits")
imdb_merged     = spark.table(f"{SILVER}.silver_imdb_merged")


############################################################
# Filter IMDB merged data to 1 of the same title per year
############################################################

# For each title and release year, order by imdbScore and imdbVotes
w_imdb = Window.partitionBy("title", "releaseYear") \
              .orderBy(col("imdbScore").desc_nulls_last(),
                       col("imdbVotes").desc_nulls_last())

# Filter to first result for each title/year             
imdb_best = (
    imdb_merged
    .withColumn("rn", row_number().over(w_imdb))
    .filter(col("rn") == 1)
    .drop("rn")
)

############################################################
# Find unique director from Netflix credits
############################################################

directors_only = (
    netflix_credits
    .filter(lower(col("role")) == "director")
)

w_dir = Window.partitionBy("id").orderBy(col("personId").asc())

directors_single = ( 
    directors_only
    .withColumn("rn", row_number().over(w_dir))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumnRenamed("name", "director")
)

############################################################
# Filter Netflix Titles to Movies
############################################################

netflix_movies_base = (
    netflix_titles
    .filter(lower(col("type")) == "movie")  # keep only movies
)

############################################################
# Left join Netflix Movies to IMDB on imdb ID
############################################################

# rename columns with identical names from different tables for easier handling later
imdb_best = (
    imdb_best
        .withColumnRenamed("imdbScore", "imdbScore_imdb")
        .withColumnRenamed("imdbVotes", "imdbVotes_imdb")
        .withColumnRenamed("imdbId", "imdbId_imdb")
)

# select only the columns we'll be using
imdb_best = imdb_best.select(
        "movieLink",
        "imdbScore_imdb",
        "imdbVotes_imdb",
        "imdbId_imdb"
)

netflix_movies_base = (
    netflix_movies_base
        .withColumnRenamed("imdbScore", "imdbScore_netflix")
        .withColumnRenamed("imdbVotes", "imdbVotes_netflix")
        .withColumnRenamed("imdbId", "imdbId_netflix")
)
netflix_imdb_joined = (
    netflix_movies_base.alias("n")
    .join(
        imdb_best.alias("i"),
        lower(col("n.imdbId_netflix")) == lower(col("i.imdbId_imdb")),
        "left"
    )
)

############################################################
# Bring in Directors
############################################################

netflix_imdb_dir = (
    netflix_imdb_joined.alias("m")
    .join(
        directors_single.alias("d"),
        col("m.id") == col("d.Id"),
        "left"
    )
)

############################################################
# Determine IMDB Score/Vote Sourcing
############################################################

movies_with_scores = (
    netflix_imdb_dir
    .withColumn(
        "imdbScore",
        coalesce(col("imdbScore_imdb"), col("imdbScore_netflix"))
    )
    .withColumn(
        "imdbVotes",
        coalesce(col("imdbVotes_imdb"), col("imdbVotes_netflix"))
    )
    # Flag for source of imdb ratings
    .withColumn(
        "imdbRatingSource",
        when(col("imdbScore_imdb").isNotNull(), "imdb").otherwise("netflix")
    )
    # Drop the original separate source rating columns
    .drop(
    "imdbScore_imdb", "imdbVotes_imdb",
    "imdbScore_netflix", "imdbVotes_netflix"
    )
)

############################################################
# Enforce one row per title, releaseYear
############################################################

# For each title and release year, order by imdbScore and imdbVotes

w_final = Window.partitionBy("m.title", "m.releaseYear") \
                .orderBy(col("imdbScore").desc_nulls_last(),
                         col("imdbVotes").desc_nulls_last())

movies_final = (
    movies_with_scores
    .withColumn("rn", row_number().over(w_final))
    .filter(col("rn") == 1)
    .drop("rn")
    .select(
        "title",
        "description",
        "releaseYear",
        "ageCertification",
        "runtime",
        "genres",
        "productionCountries",
        "movieLink",
        "imdbScore",
        "imdbVotes",
        "tmdbPopularity",
        "tmdbScore",
        "director"
        )
.filter(col("title").isNotNull())
)


#movies_with_scores.printSchema()
#movies_with_scores.columns
#display(movies_final)


movies_final.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD}.movies")