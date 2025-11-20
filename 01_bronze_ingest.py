# Databricks notebook source
############################################################
################# Ingest and Load to Bronze ################
#   - Download 3 sets of IMDB and 2 Netflix files from kaggle
#   - Copy files to bronze raw data volume
#   - Camelcase columns
#   - Add year of file to IMDB files as new column
#   - Load to bronze tables
############################################################


# %python
#from pyspark.sql.functions import *
# %pip install kagglehub
import kagglehub
import shutil
import os
import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name, regexp_extract


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


def camel_case_columns(df: DataFrame) -> DataFrame:
    new_cols = [to_camel_case(c) for c in df.columns]
    return df.toDF(*new_cols)



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

# # Download latest imdb files
imdb_path = kagglehub.dataset_download("raedaddala/imdb-movies-from-1960-to-2023")
# print("Path to imdb dataset files:", imdb_path)

target = "/Volumes/bronze_catalog/movies/raw_data/imdb"
os.makedirs(target, exist_ok=True)
copy_csvs(imdb_path, target)

# dbutils.fs.mkdirs(target)
# dbutils.fs.cp(f"file:{imdb_path}", target, recurse=True)
# imdb_raw = spark.read.csv("dbfs:/Volumes/bronze_catalog/movies/raw_data/imdb/Data/**/*.csv", header=True, inferSchema=True)
# imdb_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_imdb")


# 1) All advanced_movies_details_YYYY.csv
imdb_advanced_raw = spark.read.csv(
    f"{target}/Data/*/advanced_movies_details_*.csv",
    header=True,
    inferSchema=True,
    multiLine=True,
    quote='"',
    escape='"'
)


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



# 2) All imdb_movies_YYYY.csv
imdb_movies_raw = spark.read.csv(
    f"{target}/Data/*/imdb_movies_*.csv",
    header=True,
    inferSchema=True,
    multiLine=True,
    quote='"',
    escape='"'
)

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

# 3) All merged_movies_data_YYYY.csv
imdb_merged_raw = spark.read.csv(
    f"{target}/Data/*/merged_movies_data_*.csv",
    header=True,
    inferSchema=True,
    multiLine=True,
    quote='"',
    escape='"'
)

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


# Download latest netflix files
netflix_path = kagglehub.dataset_download("victorsoeiro/netflix-tv-shows-and-movies")
# print("Path to netflix dataset files:", netflix_path)


target = "/Volumes/bronze_catalog/movies/raw_data/netflix"
os.makedirs(target, exist_ok=True)
copy_csvs(netflix_path, target)

# dbutils.fs.mkdirs(target)
# dbutils.fs.cp(f"file:{netflix_path}", target, recurse=True)

# 1) Netflix Credits
netflix_credits_raw = spark.read.csv(
    f"{target}/credits.csv",
    header=True,
    inferSchema=True
)

netflix_credits_clean   = camel_case_columns(netflix_credits_raw)

netflix_credits_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_catalog.movies.bronze_netflix_credits")

# 2 ) Netflix Titles
netflix_titles_raw = spark.read.csv(
    f"{target}/titles.csv",
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



# for filename in os.listdir(netflix_path):
#     src = os.path.join(netflix_path, filename)
#     dst = os.path.join(target, filename)
#     if os.path.isfile(src):
#         shutil.copy2(src, dst)

# #netflix_raw = spark.read.csv("file:{netflix_path}/*.csv", header=True, inferSchema=True)

# #netflix_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_netflix")