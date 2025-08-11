"""
Automatic Table Creation from CSV Files

This script automatically scans a given folder for CSV files,
creates PostgreSQL tables named after each file (without the extension),
and imports the data from each CSV into its corresponding table.

Requirements:
- PostgreSQL server running and accessible.
- Environment variables for DB credentials set in a .env file.
- psycopg2 and pandas installed.
"""

from dotenv import load_dotenv
import psycopg2
import pandas as pd
import os

# Load environment variables from .env file
load_dotenv()

# Database connection settings
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Path to the folder containing the CSV files
CSV_FOLDER = "../item"

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT
)
conn.autocommit = True
cur = conn.cursor()


def get_pg_type(dtype):
    """
    Map pandas dtype to PostgreSQL data type.
    """
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "NUMERIC"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMPTZ"
    else:
        return "VARCHAR(255)"


# Iterate over all CSV files in the target folder
for filename in os.listdir(CSV_FOLDER):
    if not filename.endswith(".csv"):
        continue

    table_name = os.path.splitext(filename)[0]
    csv_path = os.path.join(CSV_FOLDER, filename)
    print(f"Processing file: {csv_path} ...")

    # Read CSV file into pandas DataFrame
    df = pd.read_csv(csv_path)

    # Generate column definitions with PostgreSQL types
    columns_with_types = []
    for col in df.columns:
        pg_type = get_pg_type(df[col].dtype)
        columns_with_types.append(f"{col} {pg_type}")

    # Detect unique PostgreSQL types en esta tabla
    unique_types = set()
    for col in df.columns:
        pg_type = get_pg_type(df[col].dtype)
        unique_types.add(pg_type)

    if len(unique_types) < 3:
        print(f"Warning: Table '{table_name}' has less than 3 data types\
        ({unique_types}). Skipping.")
        continue

    # Create table SQL
    create_table_sql = f"DROP TABLE IF EXISTS {table_name};\n"
    create_table_sql += f"CREATE TABLE {table_name} (\n  "
    create_table_sql += ",\n  ".join(columns_with_types)
    create_table_sql += "\n);"

    # Execute table creation
    cur.execute(create_table_sql)
    print(f"Table '{table_name}' created successfully.")

    # Insert CSV data into the table
    with open(csv_path, "r") as f:
        cur.copy_expert(f"COPY {table_name} FROM STDIN CSV HEADER", f)
    print(f"Data successfully inserted into '{table_name}'.")

# Close DB connection
cur.close()
conn.close()
print("All CSV files processed and database connection closed.")
