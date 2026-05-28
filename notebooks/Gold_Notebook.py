# Databricks notebook source
# MAGIC %md
# MAGIC Dimensions
# MAGIC 1) Author and Director -- dim_crew_members (crew_id,crew_name,crew_type)
# MAGIC 2) hero_heroine and others -- dim_performers (performer_id,performer_name,performer_type,dominant_language)
# MAGIC 3) title annd movie_name  -- dim_Name (name_id,name,language)
# MAGIC 4) dim_language -- 
# MAGIC 5) dim_genre -- (genre_id,genre)
# MAGIC 6) dim_calendar (year_id,year)
# MAGIC
# MAGIC Facts 
# MAGIC 1) fct_mv_predictions
# MAGIC cred_id,perform_id,name_id,langauge_id,genre_id,date_id,rating,budget

# COMMAND ----------

# DBTITLE 1,Table for Fact
# MAGIC %sql
# MAGIC create table mv_recomm_db.gold.fct_mv_predections (
# MAGIC         crew_id bigint,
# MAGIC         perform_id bigint,
# MAGIC         name_id bigint,
# MAGIC         langauge_id bigint,
# MAGIC         genre_id bigint,
# MAGIC         date_id bigint,
# MAGIC         rating decimal(18,4),
# MAGIC         budget decimal(18,4)
# MAGIC     )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Insert Query for Fact
from pyspark.sql.functions import col, lit, lower, trim

# ── 1. Load silver tables ──────────────────────────────────────────────────────

df_english = spark.table("mv_recomm_db.silver.english") \
    .withColumnRenamed("Movie_name", "movie_name") \
    .withColumnRenamed("Director", "director_name") \
    .withColumnRenamed("Hero", "hero_name") \
    .withColumnRenamed("Heroine", "heroine_name") \
    .withColumnRenamed("Genre", "genre") \
    .withColumnRenamed("Release_year", "release_year") \
    .withColumnRenamed("Rating", "rating") \
    .withColumnRenamed("Budget", "budget") \
    .withColumn("language", lit("English"))

df_telugu = spark.table("mv_recomm_db.silver.telugu") \
    .withColumn("language", lit("Telugu"))

df_hindi = spark.table("mv_recomm_db.silver.hindi") \
    .withColumn("language", lit("Hindi"))

df_books = spark.table("mv_recomm_db.silver.books") \
    .withColumnRenamed("title", "movie_name") \
    .withColumnRenamed("author", "director_name") \
    .withColumnRenamed("bookID", "book_id") \
    .withColumn("language", lit("English"))

# ── 2. Union all silver tables ─────────────────────────────────────────────────

df_silver = df_english \
    .unionByName(df_telugu, allowMissingColumns=True) \
    .unionByName(df_hindi, allowMissingColumns=True) \
    .unionByName(df_books, allowMissingColumns=True)

# ── 3. Clean columns before joins ──────────────────────────────────────────────

df_silver = df_silver \
    .withColumn("director_name_clean", lower(trim(col("director_name")))) \
    .withColumn("hero_name_clean", lower(trim(col("hero_name")))) \
    .withColumn("movie_name_clean", lower(trim(col("movie_name")))) \
    .withColumn("language_clean", lower(trim(col("language")))) \
    .withColumn("genre_clean", lower(trim(col("genre"))))

# ── 4. Load dimension tables ───────────────────────────────────────────────────

dim_crew = spark.table("mv_recomm_db.gold.dim_crew_members") \
    .withColumn("crew_member_name_clean",
                lower(trim(col("crew_member_name"))))

dim_perf = spark.table("mv_recomm_db.gold.dim_performers") \
    .withColumn("performer_name_clean",
                lower(trim(col("performer_name"))))

dim_name = spark.table("mv_recomm_db.gold.dim_name") \
    .withColumn("name_clean",
                lower(trim(col("name"))))

dim_lang = spark.table("mv_recomm_db.gold.dim_language") \
    .withColumn("language_clean_dim",
                lower(trim(col("language"))))

dim_genre = spark.table("mv_recomm_db.gold.dim_genre") \
    .withColumn("genre_clean_dim",
                lower(trim(col("genre"))))

dim_cal = spark.table("mv_recomm_db.gold.dim_calendar")

# ── 5. Join with dimensions ────────────────────────────────────────────────────

df_final = df_silver \
    .join(
        dim_crew,
        df_silver.director_name_clean == dim_crew.crew_member_name_clean,
        "left"
    ) \
    .join(
        dim_perf,
        df_silver.hero_name_clean == dim_perf.performer_name_clean,
        "left"
    ) \
    .join(
        dim_name,
        df_silver.movie_name_clean == dim_name.name_clean,
        "left"
    ) \
    .join(
        dim_lang,
        df_silver.language_clean == dim_lang.language_clean_dim,
        "left"
    ) \
    .join(
        dim_genre,
        df_silver.genre_clean == dim_genre.genre_clean_dim,
        "left"
    ) \
    .join(
        dim_cal,
        df_silver.release_year == dim_cal.year,
        "left"
    ) \
    .select(
        dim_crew.crew_member_id.alias("crew_id"),
        dim_perf.performer_id.alias("perform_id"),
        dim_name.name_id.alias("name_id"),
        dim_lang.language_id.alias("language_id"),
        dim_genre.genre_id.alias("genre_id"),
        dim_cal.year_id.alias("date_id"),
        df_silver.rating,
        df_silver.budget
    )

# ── 6. Preview ─────────────────────────────────────────────────────────────────

display(df_final)

# ── 7. Write fact table ────────────────────────────────────────────────────────

# df_final.write.mode("overwrite").saveAsTable(
#     "mv_recomm_db.gold.fct_mv_predictions"
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE mv_recomm_db.gold.dim_crew_members (
# MAGIC     crew_member_id INT,
# MAGIC     crew_member_name STRING
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Table for Calender
# MAGIC %sql
# MAGIC create table mv_recomm_db.gold.dim_calendar (
# MAGIC     year_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1000 INCREMENT BY 1) NOT NULL PRIMARY KEY,
# MAGIC     year int
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Populate dim_calendar
years = [(year,) for year in range(1930, 2027)]
df_years = spark.createDataFrame(years, ["year"])
df_years.createOrReplaceTempView("years")
df = spark.sql("insert into  mv_recomm_db.gold.dim_calendar(year) select year from years")


# COMMAND ----------

# DBTITLE 1,table for genre
# MAGIC %sql
# MAGIC create table mv_recomm_db.gold.dim_genre (
# MAGIC     genre_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1000 INCREMENT BY 1) NOT NULL PRIMARY KEY,
# MAGIC     genre string
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Populate Genre
# MAGIC %sql 
# MAGIC insert into mv_recomm_db.gold.dim_genre(genre)
# MAGIC Select distinct Genre from(
# MAGIC select genre from mv_recomm_db.silver.hindi
# MAGIC union all 
# MAGIC select genre from mv_recomm_db.silver.telugu
# MAGIC union all 
# MAGIC Select genre from mv_recomm_db.silver.english
# MAGIC union all 
# MAGIC select genre from mv_recomm_db.silver.books)a

# COMMAND ----------

# DBTITLE 1,DIm_Name Table
# MAGIC %sql
# MAGIC create table mv_recomm_db.gold.dim_name (
# MAGIC     name_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1000 INCREMENT BY 1) NOT NULL PRIMARY KEY,
# MAGIC     name string,
# MAGIC     language string
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Inserts to Dim_Name
# MAGIC %sql 
# MAGIC insert into mv_recomm_db.gold.dim_name(name,language)
# MAGIC select DISTINCT movie_name,'Hindi' from mv_recomm_db.silver.hindi
# MAGIC union all 
# MAGIC select DISTINCT movie_name,'Telugu' from mv_recomm_db.silver.telugu
# MAGIC union all 
# MAGIC Select DISTINCT movie_name,'English' from mv_recomm_db.silver.english
# MAGIC union all 
# MAGIC select DISTINCT title,'Books' from mv_recomm_db.silver.books
# MAGIC     
# MAGIC create 

# COMMAND ----------

# DBTITLE 1,Crew_members
# MAGIC
# MAGIC %sql
# MAGIC create table mv_recomm_db.gold.dim_crew_members(
# MAGIC     crew_member_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1000 INCREMENT BY 1) NOT NULL PRIMARY KEY,
# MAGIC     crew_member_name string,
# MAGIC     crew_member_type String
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Performers
# MAGIC %sql
# MAGIC create table mv_recomm_db.gold.dim_performers (
# MAGIC     performer_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1000 INCREMENT BY 1) NOT NULL PRIMARY KEY,
# MAGIC     performer_name string,
# MAGIC     performer_type string,
# MAGIC     dominant_language string
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Inserts for Performers
# MAGIC %sql
# MAGIC insert into mv_recomm_db.gold.dim_performers(performer_name,performer_type,dominant_language)
# MAGIC select heroine_name,'mainrole','Telugu' from mv_recomm_db.silver.telugu where heroine_name <> 'Unknown' 
# MAGIC Union 
# MAGIC select 
# MAGIC   explode(split(trim(hero_name), '&')),'mainrole','Telugu'
# MAGIC from mv_recomm_db.silver.telugu
# MAGIC union
# MAGIC select heroine_name,'mainrole','Hindi' from mv_recomm_db.silver.hindi where heroine_name <> 'Unknown' 
# MAGIC Union 
# MAGIC select 
# MAGIC   explode(split(trim(hero_name), '&')),'actor and actress','Hindi'
# MAGIC from mv_recomm_db.silver.hindi
# MAGIC UNION 
# MAGIC select Hero,'Actor','English' from mv_recomm_db.silver.english 
# MAGIC UNION 
# MAGIC select 
# MAGIC  Heroine,'actress','English'
# MAGIC from mv_recomm_db.silver.english

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_genre AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     ROW_NUMBER() OVER (ORDER BY genre) + 999 AS genre_id,
# MAGIC     genre
# MAGIC FROM (
# MAGIC
# MAGIC     SELECT genre FROM silver.telugu
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT genre FROM silver.hindi
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT genre FROM silver.english
# MAGIC
# MAGIC ) g;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fct_mv_predictions AS
# MAGIC
# MAGIC SELECT
# MAGIC     dg.genre_id,
# MAGIC     f.rating,
# MAGIC     f.budget,
# MAGIC     f.language_id,
# MAGIC     f.name_id,
# MAGIC     f.perform_id,
# MAGIC     f.crew_id,
# MAGIC     f.date_id
# MAGIC
# MAGIC FROM gold.fct_mv_predictions f
# MAGIC
# MAGIC LEFT JOIN gold.dim_genre dg
# MAGIC ON f.genre_id = dg.genre_id;