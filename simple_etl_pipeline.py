"""
simple_etl_pipeline.py

Short ETL example:
- Extract: fetch users and posts from a demo API (JSONPlaceholder)
- Transform: normalize and validate fields
- Load: store into a normalized SQLite database using SQLAlchemy
- Query: run some example SQL queries demonstrating joins, aggregates, and filtering
"""

import json
from typing import Tuple
import requests
import pandas as pd
from sqlalchemy import create_engine, text

API_BASE = "https://jsonplaceholder.typicode.com"   # demo API for learning
DB_URL = "sqlite:///pipeline.db"                     # local SQLite file

# -------------------
# 1) EXTRACT
# -------------------
def fetch_api(path: str):
    try:
        r = requests.get(f"{API_BASE}/{path}", timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Warning: API fetch {path} failed ({e}). Using fallback sample.")
        return None

def extract():
    users = fetch_api("users")
    posts = fetch_api("posts")

    # fallback small samples if API unavailable
    if users is None:
        users = [
            {"id": 1, "name": "Alice", "username": "alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "username": "bob", "email": "bob@example.com"},
        ]
    if posts is None:
        posts = [
            {"userId": 1, "id": 1, "title": "Hello", "body": "First post"},
            {"userId": 1, "id": 2, "title": "World", "body": "Second post"},
            {"userId": 2, "id": 3, "title": "Another", "body": "Third post"},
        ]

    return pd.DataFrame(users), pd.DataFrame(posts)

# -------------------
# 2) TRANSFORM (clean / normalize)
# -------------------
def transform(users_df: pd.DataFrame, posts_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Normalize user table: keep id, name, username, email
    users = users_df[['id', 'name', 'username', 'email']].copy()
    users.columns = ['user_id', 'name', 'username', 'email']

    # Basic validation: remove rows with missing user_id or email
    users = users.dropna(subset=['user_id'])
    users['email'] = users['email'].fillna('unknown@example.com')

    # Normalize posts: keep id, userId -> user_id, title, body
    posts = posts_df[['id', 'userId', 'title', 'body']].copy()
    posts.columns = ['post_id', 'user_id', 'title', 'body']

    # Ensure types are correct
    posts['post_id'] = pd.to_numeric(posts['post_id'], errors='coerce').astype('Int64')
    posts['user_id'] = pd.to_numeric(posts['user_id'], errors='coerce').astype('Int64')

    # Data quality: drop posts without user_id or title
    posts = posts.dropna(subset=['post_id', 'user_id', 'title'])
    posts['title'] = posts['title'].astype(str).str.strip()

    # Example transformation: add title length as derived column
    posts['title_len'] = posts['title'].str.len()

    return users, posts

# -------------------
# 3) LOAD into SQLite (normalized schema: users, posts)
# -------------------
def load_to_sqlite(users: pd.DataFrame, posts: pd.DataFrame, db_url: str = DB_URL):
    engine = create_engine(db_url, echo=False, future=True)

    # Write tables (replace existing for demo)
    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON;"))   # enable FKs in SQLite
        users.to_sql('users', conn, if_exists='replace', index=False)
        posts.to_sql('posts', conn, if_exists='replace', index=False)

        # add simple foreign key enforcement in SQLite (via a small migration)
        # SQLite requires table recreation for FK constraints; for brevity we skip strict FK DDL here.
        # In production, define schema with migrations (alembic / CREATE TABLE with FK).

    print(f"Loaded {len(users)} users and {len(posts)} posts into {db_url}")

# -------------------
# 4) EXAMPLE SQL QUERIES (analysis)
# -------------------
def run_example_queries(db_url: str = DB_URL):
    engine = create_engine(db_url, echo=False, future=True)
    with engine.connect() as conn:
        print("\n1) Top 5 users by number of posts:")
        q1 = """
        SELECT u.user_id, u.username, COUNT(p.post_id) AS post_count
        FROM users u
        LEFT JOIN posts p ON u.user_id = p.user_id
        GROUP BY u.user_id, u.username
        ORDER BY post_count DESC
        LIMIT 5;
        """
        print(pd.read_sql(text(q1), conn).to_string(index=False))

        print("\n2) Average title length per user (descending):")
        q2 = """
        SELECT u.user_id, u.username, AVG(p.title_len) AS avg_title_len
        FROM users u
        JOIN posts p ON u.user_id = p.user_id
        GROUP BY u.user_id, u.username
        ORDER BY avg_title_len DESC
        LIMIT 10;
        """
        print(pd.read_sql(text(q2), conn).to_string(index=False))

        print("\n3) Example of a complex filter: posts with short titles (<10 chars):")
        q3 = "SELECT post_id, user_id, title, title_len FROM posts WHERE title_len < 10 ORDER BY title_len ASC LIMIT 10;"
        print(pd.read_sql(text(q3), conn).to_string(index=False))

# -------------------
# 5) SIMPLE ETL RUN
# -------------------
def main():
    users_raw, posts_raw = extract()
    users, posts = transform(users_raw, posts_raw)
    load_to_sqlite(users, posts)
    run_example_queries()

    # Show how to fetch data programmatically (API -> DB) as a completed pipeline step
    print("\nETL pipeline finished successfully. Database file: pipeline.db")

if __name__ == "__main__":
    main()
