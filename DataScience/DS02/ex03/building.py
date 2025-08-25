import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import sys
import os
sys.path.append("../../DS01/ex01")
from customers_table import load_env_vars, connect_db
sys.path.append("../../DS01/ex02")
from remove_duplicates import select_table
sys.path.append("../../DS02/ex01")
from chart import table_to_dataframe

def plot_price_histogram(df):
    """
    Plot bar chart of total Altairian Dollars spent per user.
    """
    df_spent_per_user = df.groupby(df['user_id'])['price'].sum().reset_index()

    plt.figure(figsize=(8, 6))
    plt.hist(df_spent_per_user['price'], bins=range(0, 251, 50), edgecolor='black')
    plt.xlabel("Monetary value")
    plt.ylabel("Customerss")
    plt.title("Spent per user")
    plt.show()


def plot_event_histogram(df):
    """
    Plot histogram of number of events per user.
    """
    events_per_user = df['user_id'].value_counts()

    plt.figure(figsize=(8, 6))
    plt.hist(events_per_user, bins=range(0, 41, 10), edgecolor='black')
    plt.xlabel("Frequency")
    plt.ylabel("Customerss")
    plt.title("Events per client distribution")
    plt.show()


def main():
    """
    1. Load database credentials.
    2. Connect to PostgreSQL.
    3. Let the user select a table and column.
    4. Retrieve data and plot two histograms.
    """

    try:
        env_path = "../../DS01/ex00/.env"
        db_config = load_env_vars(env_path)
        conn, cur = connect_db(db_config)
        print("Select the table you want to take data from: ")
        table = select_table(cur)
        df = table_to_dataframe(cur, table)
        
        plot_event_histogram(df)
        plot_price_histogram(df)
        

    finally:
        cur.close()
        conn.close()
        print("Database connection closed.") 


if __name__ == "__main__":
    main()