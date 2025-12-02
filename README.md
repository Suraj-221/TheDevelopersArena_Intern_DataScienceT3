# Month 3: Database Management & API ETL Pipeline (Python)

## Overview
This project demonstrates a complete mini data-engineering workflow using Python.  
It includes API extraction, data cleaning, normalization, loading into a database, and running SQL analysis queries.

The script is intentionally short, easy to understand, and designed for beginners learning SQL, ETL, and API-based data pipelines.

---

## Features Covered

### 1. Database & SQL Concepts
- Creating normalized database tables  
- Loading data into relational databases  
- Writing SQL joins, aggregations, filtering, and sorting  
- Using SQLite (simple, no setup required)

### 2. API & Web Data Concepts
- Fetching real JSON data from APIs  
- Handling API failures with fallback sample data  
- Parsing JSON to DataFrames  
- Validating and cleaning raw API data

### 3. ETL (Extract → Transform → Load)
- **Extract:** Calling an API (`jsonplaceholder.typicode.com`)  
- **Transform:** Normalizing tables, validating fields, adding derived columns  
- **Load:** Writing into SQLite database (`pipeline.db`)  
- **Analyze:** Running SQL queries using SQLAlchemy  

### 4. Example SQL Queries Included
The script shows:
- Top users by number of posts  
- Average post title length per user  
- Posts filtered by short titles  
- Join operations across tables  

---

## Requirements

Install required packages:

```bash
pip install pandas sqlalchemy requests
```

SQLite is built into Python, so no extra installation is needed.

---

## How to Run

1. Save the script as:
```
simple_etl_pipeline.py
```

2. Run it:
```bash
python simple_etl_pipeline.py
```

3. After running, you will get:
- A generated SQLite database file:  
  ```
  pipeline.db
  ```
- SQL analysis output printed in the terminal  
- Clean normalized tables (`users` and `posts`) stored in the DB

---

## File Structure
```
simple_etl_pipeline.py     # Main ETL pipeline script
pipeline.db                # Auto-generated SQLite database
README.md                  # Project documentation
```

---

## Customization

| Goal | How to Modify |
|------|---------------|
| Use real APIs | Replace API_BASE with your own API endpoint |
| Add more tables | Extend `transform()` and `load_to_sqlite()` |
| Use MySQL/PostgreSQL | Change DB_URL connection string |
| Add web scraping | Add a scraper inside `extract()` function |
| Build full pipelines | Wrap ETL steps in cron jobs / scheduler |
| Add dashboards | Load final tables into PowerBI, Tableau, or Python dashboards |

---

## Educational Purpose
This project is designed to help learners understand:
- How APIs connect to databases  
- How ETL pipelines are designed  
- How SQL queries analyze processed data  
- How to validate & transform raw data into usable format  

It is ideal for:
- Student submissions  
- Portfolio building  
- Practice for data engineering interviews  
- Mini-projects and college assignments  

---

## Author
This README.md documents the Month-3 ETL pipeline demonstration created for learning and practical skill-building.
