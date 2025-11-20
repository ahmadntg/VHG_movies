# FTC Data Engineer Technical Task - Movies Dataset Exercise

This repository contains my solution to the FTC Data Engineer technical task.

## Structure

### Bronze Layer

Ingests all Netflix and IMDB CSVs as Delta tables

CamelCase column normalisation

IMDB fileYear extraction

No filtering or business logic

### Silver Layer

Cleans and normalises the raw tables

Fixes data types

Normalises IMDB votes (K/M suffixes → full integers)

IMDB limited to 2010–2025 as per the specification

Prepares Netflix and IMDB for joining

### Gold Layer

Builds the conformed Movies table

Netflix drives the universe of movies

IMDB data enriches where available (IMDB-first, Netflix-fallback)

Directors normalised (earliest person_id per film)

One row per title + releaseYear

Fully filterable model
