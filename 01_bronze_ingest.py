# Databricks notebook source
# MAGIC %md
# MAGIC ## 01_Bronze_Ingest
# MAGIC    - Download 3 sets of IMDB and 2 Netflix files from kaggle
# MAGIC    - Copy files to bronze raw data volume
# MAGIC    - Camelcase columns
# MAGIC    - Add year of file to IMDB files as new column
# MAGIC    - Load to bronze tables
# MAGIC
# MAGIC ### Assumptions
# MAGIC - Bronze tables should contain all data, not filtered by year ("The system should ingest all available CSVs into delta tables")
# MAGIC - Initilise script has ran to setup volumes etc
# MAGIC - Kagglehub library installed
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports, Functions and intilisations

# COMMAND ----------

import kagglehub
import shutil
import os
import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name, regexp_extract

# Function: CamelCase given string
def to_camel_case(col_name: str) -> str:
    # Lowercase and strip
    col = col_name.strip().lower()
    
    # Replace invalid punctuation with space
    col = re.sub(r"[^\w]+", " ", col)    # keep letters/numbers/underscore
    
    # Split on whitespace or underscore
    parts = re.split(r"[_\s]+", col)
    parts = [p for p in parts if p]      # drop empty
    
    if not parts:
        return col_name  # fallback
    
    # First part stays lowercase, rest capitalised
    camel = parts[0] + "".join(p.capitalize() for p in parts[1:])
    
    return camel

# Function: Pass columns for Camel Casing
def camel_case_columns(df: DataFrame) -> DataFrame:
    new_cols = [to_camel_case(c) for c in df.columns]
    return df.toDF(*new_cols)


# Function: Copy files
def copy_csvs(src_root, dst_root):
    for root, dirs, files in os.walk(src_root):
        # preserve relative structure 
        rel = os.path.relpath(root, src_root)
        dst_dir = os.path.join(dst_root, rel) if rel != "." else dst_root
        os.makedirs(dst_dir, exist_ok=True)
        for f in files:
            if f.lower().endswith(".csv"):
                src = os.path.join(root, f)
                dst = os.path.join(dst_dir, f)
                shutil.copy2(src, dst)

# Bronze volume location for files to be copied to and read from
target_imdb = "/Volumes/bronze_catalog/movies/raw_data/imdb"
target_netflix = "/Volumes/bronze_catalog/movies/raw_data/netflix"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Download latest imdb and Netflix files

# COMMAND ----------

# Download latest imdb files
imdb_path = kagglehub.dataset_download("raedaddala/imdb-movies-from-1960-to-2023")
# print("Path to imdb dataset files:", imdb_path)

# Download latest netflix files
netflix_path = kagglehub.dataset_download("victorsoeiro/netflix-tv-shows-and-movies")
# print("Path to netflix dataset files:", netflix_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy downloaded files to bronze volume

# COMMAND ----------

os.makedirs(target_imdb, exist_ok=True)
copy_csvs(imdb_path, target_imdb)


os.makedirs(target_netflix, exist_ok=True)
copy_csvs(netflix_path, target_netflix)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write files to bronze tables

# COMMAND ----------


# 1) IMDB - All advanced_movies_details_YYYY.csv
imdb_advanced_raw = spark.read.csv(
    f"{target_imdb}/Data/*/advanced_movies_details_*.csv",
    header=True,
    inferSchema=True,
    multiLine=True,
    quote='"',
    escape='"'
)

# Camel case columns and add fileYear based on filename
imdb_advanced_clean = camel_case_columns(imdb_advanced_raw).withColumn(
    "fileYear",
    regexp_extract(
        "_metadata.file_path",
        r"/Data/(\d{4})/",
        1
    )
)

imdb_advanced_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_catalog.movies.bronze_imdb_advanced")



# 2) IMDB - All imdb_movies_YYYY.csv
imdb_movies_raw = spark.read.csv(
    f"{target_imdb}/Data/*/imdb_movies_*.csv",
    header=True,
    inferSchema=True,
    multiLine=True,
    quote='"',
    escape='"'
)

# Camel case columns and add fileYear based on filename
imdb_movies_clean = camel_case_columns(imdb_movies_raw).withColumn(
    "fileYear",
    regexp_extract(
        "_metadata.file_path",
        r"/Data/(\d{4})/",
        1
    )
)

imdb_movies_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_catalog.movies.bronze_imdb_movies")

# 3) IMDB - All merged_movies_data_YYYY.csv
imdb_merged_raw = spark.read.csv(
    f"{target_imdb}/Data/*/merged_movies_data_*.csv",
    header=True,
    inferSchema=True,
    multiLine=True,
    quote='"',
    escape='"'
)

# Camel case columns and add fileYear based on filename
imdb_merged_clean   = camel_case_columns(imdb_merged_raw).withColumn(
    "fileYear",
    regexp_extract(
        "_metadata.file_path",
        r"/Data/(\d{4})/",
        1
    )
)

imdb_merged_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_catalog.movies.bronze_imdb_merged")


# 4) Netflix Credits
netflix_credits_raw = spark.read.csv(
    f"{target_netflix}/credits.csv",
    header=True,
    inferSchema=True
)

netflix_credits_clean   = camel_case_columns(netflix_credits_raw)

netflix_credits_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_catalog.movies.bronze_netflix_credits")

# 5 ) Netflix Titles
netflix_titles_raw = spark.read.csv(
    f"{target_netflix}/titles.csv",
    header=True,
    inferSchema=True,
    multiLine=True,
    quote='"',
    escape='"'
)

netflix_titles_clean   = camel_case_columns(netflix_titles_raw)

netflix_titles_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_catalog.movies.bronze_netflix_titles")

