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
sys.path.append("../../DS01/ex01")
from chart import table_to_dataframe

def mean(data):
        return sum(data) / len(data)


def median(data):

    n = len(data)
    mid = n // 2
    if n % 2 == 0:
        return (data[mid - 1] + data[mid]) / 2
    else:
        return data[mid]

def quartile(data, qtil: int):
    n = len(data)
    q1 = n // 4
    q = qtil * q1
    return data[q]

def count(data):
    return len(data)


def mini(data):
    return data[0]


def maxi(data):
    return data[len(data) - 1]
    

def plot_avg_box_chart(df_purchases):
    avg_per_user = df_purchases.groupby('user_id')['price'].mean()

    plt.figure(figsize=(4, 4))
    plt.boxplot(avg_per_user, vert=False,
            patch_artist=True,
            boxprops=dict(facecolor='blue', alpha=0.5),
            medianprops=dict(color='black', linewidth=2),
            showfliers=False)
    plt.xlabel("Average basket price")
    plt.title('Box plot of purchased item prices')
    plt.show()


def plot_q_box_chart(series):
    plt.figure(figsize=(4, 4))
    plt.boxplot(series, vert=False,
            patch_artist=True,
            boxprops=dict(facecolor='green', color='green', alpha=0.5),
            medianprops=dict(color='black', linewidth=2),
            showfliers=False)
    plt.xlabel('Price')
    plt.title('Box plot of purchased item prices')
    plt.show()


def plot_box_chart(series):
    plt.figure(figsize=(4, 4))
    plt.boxplot(series, vert=False)
    plt.xlabel('Price')
    plt.title('Box plot of purchased item prices')
    plt.show()

def main():
    """
    1. Load database credentials.
    2. Connect to PostgreSQL.
    3. Let the user select a table and column.
    4. Retrieve data and print stadistic calculations.
    5. Prints box charts
    """

    try:
        env_path = "../../DS01/ex00/.env"
        db_config = load_env_vars(env_path)
        conn, cur = connect_db(db_config)
        print("Select the table you want to take data from: ")
        table = select_table(cur)
        df = table_to_dataframe(cur, table)
        prices_series = df['price'].astype(float)

        prices_list = prices_series.tolist()
        price_sorted = sorted(prices_list)

        print(f"count : {count(price_sorted)}")
        print(f"mean : {mean(price_sorted)}")
        print(f"median : {median(price_sorted)}")
        print(f"min : {mini(price_sorted)}")
        print(f"25% : {quartile(price_sorted, 1)}")
        print(f"50% : {quartile(price_sorted, 2)}")
        print(f"75% : {quartile(price_sorted, 3)}")
        print(f"max : {maxi(price_sorted)}")

        plot_box_chart(prices_series)
        plot_q_box_chart(prices_series)
        plot_avg_box_chart(df)

    finally:
        cur.close()
        conn.close()
        print("Database connection closed.") 


if __name__ == "__main__":
    main()