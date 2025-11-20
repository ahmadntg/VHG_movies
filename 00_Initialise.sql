CREATE CATALOG IF NOT EXISTS bronze_catalog;
CREATE SCHEMA IF NOT EXISTS bronze_catalog.movies;
CREATE VOLUME IF NOT EXISTS bronze_catalog.movies.raw_data;

CREATE CATALOG IF NOT EXISTS silver_catalog;
CREATE SCHEMA IF NOT EXISTS silver_catalog.movies;

CREATE CATALOG IF NOT EXISTS gold_catalog;
CREATE SCHEMA IF NOT EXISTS gold_catalog.movies;