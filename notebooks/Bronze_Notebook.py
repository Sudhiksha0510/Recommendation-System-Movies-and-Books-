# Databricks notebook source
# MAGIC %md
# MAGIC Table Scripts for all Bronze tables 
# MAGIC 1) Books_Selling 
# MAGIC 2) Telugu Movies
# MAGIC 3) Hindi Movies 
# MAGIC 4) English Movies 
# MAGIC 5) Telugu Movies 2
# MAGIC 6) Hindi Movies 2
# MAGIC 7) Hindi Movies 3

# COMMAND ----------

spark.sql("""
  CREATE TABLE IF NOT EXISTS bronze.books_selling
  select * from parquet.`abfss://bronze@storedevstore01.dfs.core.windows.net/books/best_selling_books_clean.parquet`
""")

# COMMAND ----------

spark.read.parquet(
  "abfss://bronze@storedevstore01.dfs.core.windows.net/telugu/Telugu_movies_1992_2019.parquet"
).write.mode("ignore").saveAsTable("bronze.telugu_movies")

# COMMAND ----------

# DBTITLE 1,Untitled
spark.read.parquet(
  "abfss://bronze@storedevstore01.dfs.core.windows.net/hindi/bollywood_movies.parquet"
).write.mode("ignore").saveAsTable("bronze.hindi_movies")

# COMMAND ----------

spark.read.parquet(
  "abfss://bronze@storedevstore01.dfs.core.windows.net/hindi/Hindi_Movies_Dataset.parquet"
).write.mode("ignore").saveAsTable("bronze.hindi_movies_2")

# COMMAND ----------

spark.read.parquet(
  "abfss://bronze@storedevstore01.dfs.core.windows.net/hindi/BollywoodMovieDetail.parquet"
).write.mode("ignore").saveAsTable("bronze.hindi_movies_3")

# COMMAND ----------

spark.read.parquet(
  "abfss://bronze@storedevstore01.dfs.core.windows.net/english/hollywood_movies.parquet"
).write.mode("ignore").saveAsTable("bronze.english_movies")

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mv_recomm_db.bronze.telugu_movies AS
# MAGIC SELECT
# MAGIC   Film_Name,
# MAGIC   Director,
# MAGIC   Hero,
# MAGIC   Rating,
# MAGIC   Genre,
# MAGIC   Release_Year,
# MAGIC   Heroine,
# MAGIC   Budget_Crores,
# MAGIC   OTT_Platform
# MAGIC FROM mv_recomm_db.bronze.telugu_movies