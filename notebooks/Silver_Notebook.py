# Databricks notebook source
# MAGIC %md
# MAGIC Table Scripts for all Silver tables 
# MAGIC 1) Books
# MAGIC 2) Telugu Movies 
# MAGIC 3) Hindi Movies 
# MAGIC 4) English Movies 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver.books AS
# MAGIC WITH selling_dedup AS (
# MAGIC   SELECT
# MAGIC     book_title,
# MAGIC     author,
# MAGIC     genre,
# MAGIC     rating,
# MAGIC     lower(regexp_replace(trim(book_title), '[^a-z0-9]', '')) AS norm_title,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY lower(regexp_replace(trim(book_title), '[^a-z0-9]', ''))
# MAGIC       ORDER BY book_title
# MAGIC     ) AS rn
# MAGIC   FROM bronze.books_selling
# MAGIC ),
# MAGIC books_enriched AS (
# MAGIC   SELECT
# MAGIC     b.title,
# MAGIC     b.authors AS author,
# MAGIC     s.genre,
# MAGIC     b.average_rating AS rating
# MAGIC   FROM bronze.books b
# MAGIC   LEFT JOIN selling_dedup s
# MAGIC     ON lower(regexp_replace(trim(b.title), '[^a-z0-9]', '')) = s.norm_title
# MAGIC    AND s.rn = 1
# MAGIC ),
# MAGIC selling_unmatched AS (
# MAGIC   SELECT
# MAGIC     s.book_title AS title,
# MAGIC     s.author,
# MAGIC     s.genre,
# MAGIC     s.rating
# MAGIC   FROM selling_dedup s
# MAGIC   LEFT ANTI JOIN (
# MAGIC     SELECT DISTINCT lower(regexp_replace(trim(title), '[^a-z0-9]', '')) AS norm_title
# MAGIC     FROM bronze.books
# MAGIC   ) b
# MAGIC     ON s.norm_title = b.norm_title
# MAGIC   WHERE s.rn = 1
# MAGIC )
# MAGIC SELECT title, author, genre, rating
# MAGIC FROM books_enriched
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT title, author, genre, rating
# MAGIC FROM selling_unmatched;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.telugu AS
# MAGIC
# MAGIC SELECT
# MAGIC     Film_Name                    AS movie_name,
# MAGIC     Director                     AS director_name,
# MAGIC     Hero                         AS hero_name,
# MAGIC     Heroine                      AS heroine_name,
# MAGIC     Genre                        AS genre,
# MAGIC     CAST(Release_Year AS INT)    AS release_year,
# MAGIC     CAST(Rating AS DOUBLE)       AS rating,
# MAGIC     CAST(Budget_Crores AS DOUBLE) AS budget
# MAGIC FROM bronze.telugu_movies
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     Title                        AS movie_name,
# MAGIC     Director                     AS director_name,
# MAGIC     Cast                         AS hero_name,
# MAGIC     'Unknown'                    AS heroine_name,
# MAGIC     Genre                        AS genre,
# MAGIC     CAST(Year AS INT)            AS release_year,
# MAGIC     CAST(NULL AS DOUBLE)         AS rating,
# MAGIC     CAST(NULL AS DOUBLE)         AS budget
# MAGIC FROM bronze.telugu_movies_2;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.hindi AS
# MAGIC
# MAGIC -- TABLE 2
# MAGIC SELECT
# MAGIC     Film_Name                                      AS movie_name,
# MAGIC
# MAGIC     COALESCE(Director, 'Unknown')                  AS director_name,
# MAGIC
# MAGIC     COALESCE(Hero, 'Unknown')                      AS hero_name,
# MAGIC
# MAGIC     COALESCE(Heroine, 'Unknown')                   AS heroine_name,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         Genre,
# MAGIC         CASE pmod(abs(hash(Film_Name)), 6)
# MAGIC             WHEN 0 THEN 'Action'
# MAGIC             WHEN 1 THEN 'Comedy'
# MAGIC             WHEN 2 THEN 'Romance'
# MAGIC             WHEN 3 THEN 'Thriller'
# MAGIC             WHEN 4 THEN 'Drama'
# MAGIC             ELSE 'Fantasy'
# MAGIC         END
# MAGIC     )                                              AS genre,
# MAGIC
# MAGIC     CAST(Release_Year AS INT)                      AS release_year,
# MAGIC
# MAGIC     COALESCE(CAST(Rating AS DOUBLE), 7.0)          AS rating,
# MAGIC
# MAGIC     COALESCE(CAST(Budget_Crore AS DOUBLE), 100.0)  AS budget
# MAGIC
# MAGIC FROM bronze.hindi_movies_2
# MAGIC
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC
# MAGIC -- TABLE 3
# MAGIC Select
# MAGIC     title AS movie_name,
# MAGIC
# MAGIC     COALESCE(directors, 'Unknown') AS director_name,
# MAGIC
# MAGIC     COALESCE(get(split(actors, '\\|'), 0), 'Unknown') AS hero_name,
# MAGIC
# MAGIC COALESCE(get(split(actors, '\\|'), 1), 'Unknown') AS heroine_name,
# MAGIC     COALESCE(
# MAGIC         genre,
# MAGIC         CASE pmod(abs(hash(title)), 6)
# MAGIC             WHEN 0 THEN 'Action'
# MAGIC             WHEN 1 THEN 'Comedy'
# MAGIC             WHEN 2 THEN 'Romance'
# MAGIC             WHEN 3 THEN 'Thriller'
# MAGIC             WHEN 4 THEN 'Drama'
# MAGIC             ELSE 'Fantasy'
# MAGIC         END
# MAGIC     )                                              AS genre,
# MAGIC
# MAGIC     CAST(releaseYear AS INT)                       AS release_year,
# MAGIC
# MAGIC     7.5                                            AS rating,
# MAGIC
# MAGIC     ROUND((pmod(abs(hash(imdbId)), 250) + 30), 2) AS budget
# MAGIC
# MAGIC FROM bronze.hindi_movies_3;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.english AS
# MAGIC
# MAGIC SELECT
# MAGIC     Film_Name     AS Movie_name,
# MAGIC     Director   ,
# MAGIC     Hero         ,
# MAGIC     Heroine       ,
# MAGIC     Genre         ,
# MAGIC     CAST(Release_Year AS INT) AS Release_year,
# MAGIC     CAST(Rating AS DOUBLE) AS Rating,
# MAGIC     Budget_USD    AS Budget
# MAGIC
# MAGIC FROM bronze.english_movies;