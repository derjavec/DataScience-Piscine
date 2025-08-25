import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

sys.path.append("../../DS01/ex01")
from customers_table import load_env_vars, connect_db
sys.path.append("../../DS01/ex02")
from remove_duplicates import select_table
sys.path.append("../../DS01/ex03")
from fusion import choose_column

max_unique = 20

def get_table_columns(cur, table):
    """
    Retrieve all column names from a specified table in the database.
    """
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s;
    """
    cur.execute(query, (table,))
    cols_set = {row[0] for row in cur.fetchall()}
    return cols_set



def get_data_from_column(conn, cur, col, table):
    """
    Query a PostgreSQL table to count occurrences of each unique value in a column.
    """
    query = f"""
        SELECT {col}, COUNT(*) as count
        FROM {table}
        GROUP BY {col}
    """
    df = pd.read_sql(query, conn)
    return df


def plot_pie_chart(df, col_name: str, table: str):
    """
    Plot a pie chart showing the distribution of values in a column.
    """
    values = df[col_name]
    counts = df["count"]
    if len(values) > max_unique:
        choice = input(f"Column '{col_name}' has {len(values)} unique values. "
                   "Plot anyway? (y/n): ").lower()
        if choice != 'y':
            return 

    plt.figure(figsize=(8, 8))
    plt.pie(counts, labels=values, autopct="%1.1f%%", startangle=90)
    plt.title(f"Distribution of values in '{col_name}' (Table {table})")
    plt.show()


def main():
    """
    Main function to:
    1. Load database credentials.
    2. Connect to PostgreSQL.
    3. Let the user select a table and column.
    4. Retrieve data and plot a pie chart.
    """
    try:
        env_path = "../../DS01/ex00/.env"
        db_config = load_env_vars(env_path)
        conn, cur = connect_db(db_config)
        print("Select the table you want to take data from: ")
        table = select_table(cur)
        cols_set = get_table_columns(cur, table)
        col = choose_column(cols_set, "Please select the column you want to take data from:")
        df = get_data_from_column(conn, cur, col, table)
        plot_pie_chart(df, col, table)
    finally:
        cur.close()
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
