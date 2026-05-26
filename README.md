# Recommendation-System-Movies-and-Books-
## Azure Databricks Medallion Architecture using PySpark & SQL

---

# 📌 Project Overview

This project demonstrates an end-to-end **Data Engineering Pipeline** built using the **Medallion Architecture** approach in Microsoft Azure Databricks.

The pipeline processes raw **Movies and Books datasets** through multiple layers:

```text id="vx9f0e"
Bronze  →  Silver  →  Gold
```

and transforms the data into a structured **Star Schema** consisting of:

* Dimension Tables
* Fact Tables
* Analytical Models

for reporting and business insights.

---

# 🏗️ Architecture

```text id="jlwm7p"
Raw Datasets
      │
      ▼
┌────────────┐
│ Bronze     │
│ Raw Data   │
└────────────┘
      │
      ▼
┌────────────┐
│ Silver     │
│ Cleaned &  │
│ Standardized│
└────────────┘
      │
      ▼
┌────────────┐
│ Gold       │
│ Dimensions │
│ + Facts    │
└────────────┘
```

---

# ⚙️ Technologies Used

* Python
* PySpark
* SQL
* Azure Databricks
* Delta Lake
* Data Warehousing
* Star Schema Modeling

---

# 🥉 Bronze Layer

The Bronze layer stores raw ingested data from movie and book datasets without transformations.

### Tasks Performed

* Raw file ingestion
* Schema preservation
* Initial data storage

### Example Tables

```text id="x8u0ei"
bronze.telugu_movies
bronze.books
bronze.hindi_movies
bronze.englis_hmovies
```

---

# 🥈 Silver Layer

The Silver layer cleans and transforms the raw datasets.

### Tasks Performed

* Null value handling
* Duplicate removal
* Data type conversion
* Standardization
* Transformation logic

### Example Tables

```text id="jlwm4n"
silver.english
silver.books
silver.telugu
silver.hindi
```

---

# 🥇 Gold Layer

The Gold layer contains analytical business-ready tables.

It includes:

* Dimension tables
* Fact tables
* Star schema design

---

# 📊 Dimension Tables

| Table Name         | Description                  |
| ------------------ | ---------------------------- |
| `dim_calendar`     | Stores year/date information |
| `dim_crew_members` | Stores movie crew details    |
| `dim_genre`        | Stores genres                |
| `dim_language`     | Stores languages             |
| `dim_name`         | Stores movie/book names      |
| `dim_performers`   | Stores performers/authors    |

---

# 📈 Fact Table

## `fct_mv_predictions`

This table connects all dimensions using surrogate keys and stores measurable business metrics.

### Columns

| Column         | Description                          |
| -------------- | ------------------------------------ |
| `crew_id`      | Foreign key from crew dimension      |
| `performer_id` | Foreign key from performer dimension |
| `name_id`      | Foreign key from title dimension     |
| `language_id`  | Foreign key from language dimension  |
| `genre_id`     | Foreign key from genre dimension     |
| `date_id`      | Foreign key from calendar dimension  |
| `rating`       | Movie/Book rating                    |
| `budget`       | Budget or revenue-related metric     |

---

# ⭐ Star Schema Design

```text id="g13uln"
                 dim_calendar
                       │
                       │
dim_genre ─── fct_mv_predictions ─── dim_language
                       │
                       │
            dim_performers
                       │
                       │
              dim_crew_members
                       │
                       │
                   dim_name
```

---

# 🔍 Sample Analytics Queries

## Average Rating by Genre

```sql id="wl0d69"
SELECT
    g.genre,
    AVG(f.rating) AS avg_rating
FROM gold.fct_mv_predictions f
JOIN gold.dim_genre g
ON f.genre_id = g.genre_id
GROUP BY g.genre;
```

---

## Top Performer / Author by Budget

```sql id="mhn6bp"
SELECT
    p.performer_name,
    SUM(f.budget) AS total_budget
FROM gold.fct_mv_predictions f
JOIN gold.dim_performers p
ON f.performer_id = p.performer_id
GROUP BY p.performer_name
ORDER BY total_budget DESC;
```

---

# ✅ Features Implemented

* End-to-end ETL pipeline
* Medallion architecture
* Data cleaning and transformation
* Star schema modeling
* Dimension & fact table creation
* Delta table storage
* Analytical querying

---

# 📚 Learning Outcomes

Through this project, We learned:

* ETL pipeline development
* Data warehousing concepts
* Star schema modeling
* Fact and dimension design
* PySpark transformations
* Databricks SQL workflows
* Medallion architecture implementation

---

# 🚀 Future Enhancements

* Real-time streaming ingestion
* Slowly Changing Dimensions (SCD)
* Dashboard integration using Power BI
* Workflow orchestration
* Data quality validation

---

# 👩‍💻 Author

Developed as a hands-on Data Engineering project using Azure Databricks, PySpark, and SQL for Movies & Books analytical data warehousing.
