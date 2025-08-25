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
REQUIRED_COLUMNS = {"event_time", "event_type", "price", "user_id"}

def table_to_dataframe(cur, table):
    """
    Load only required columns from the table using psycopg2 cursor
    and convert to DataFrame.
    """
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s
    """, (table,))

    available_columns = {row[0] for row in cur.fetchall()}

    missing = set(REQUIRED_COLUMNS) - available_columns
    if missing:
        raise ValueError(f"Table '{table}' is missing required columns: {', '.join(missing)}")

    query = f"""
        SELECT event_time, event_type, price, user_id
        FROM {table}
        WHERE event_type = 'purchase'
    """

    cur.execute(query)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=['event_time', 'event_type', 'price', 'user_id'])

    if df.empty:
        raise ValueError(f"No 'purchase' events found in table '{table}'.")

    return df



def plot_line_chart(df):
    """
    Plot a line chart showing unique users per month.
    """
    df['event_time'] = pd.to_datetime(df['event_time'])
    df_count = df.groupby(df['event_time'].dt.date)['user_id'].nunique().reset_index()
    df_count.rename(columns={'event_time': 'date', 'user_id': 'unique_users'}, inplace=True)

    plt.figure(figsize=(12, 6))
    plt.plot(df_count['date'], df_count['unique_users'])

    plt.ylabel("Number of customers")
    plt.title("Unique customers per day")

    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()



def plot_histogram_chart(df):
    """
    Plot an histogram chart showing total sales in millions per month.
    """
    df['event_time'] = pd.to_datetime(df['event_time'])
    df['year_month'] = df['event_time'].dt.to_period('M')
    df_sum = df.groupby('year_month')['price'].sum().reset_index()
    df_sum['price'] = df_sum['price'] / 1_000_000

    plt.figure(figsize=(10, 6))
    plt.bar(df_sum['year_month'].astype(str), df_sum['price'], color='skyblue')
    plt.ylabel("Total sales in millions")
    plt.title("Monthly Sales Histogram")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def plot_area_chart(df):
    """
    Plot an area chart showing average revenue per user (ARPU) per day,
    """

    df['event_time'] = pd.to_datetime(df['event_time'])

    df_price = df.groupby(df['event_time'].dt.date)['price'].sum().reset_index()
    df_price.rename(columns={'event_time': 'date', 'price': 'total_sales'}, inplace=True)

    df_userid = df.groupby(df['event_time'].dt.date)['user_id'].nunique().reset_index()
    df_userid.rename(columns={'event_time': 'date', 'user_id': 'unique_users'}, inplace=True)

    df_avg = pd.merge(df_price, df_userid, on='date')
    df_avg['ARPU'] = df_avg['total_sales'] / df_avg['unique_users']

    plt.figure(figsize=(12, 6))
    plt.fill_between(df_avg['date'], df_avg['ARPU'], color='skyblue', alpha=0.5)
    plt.plot(df_avg['date'], df_avg['ARPU'], color='blue')
    plt.ylabel("Average spend/customers")
    plt.xlabel("Date")
    plt.title("ARPU Over Time")

    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

   


def main():
    """
    1. Load database credentials.
    2. Connect to PostgreSQL.
    3. Let the user select a table.
    4. Retrieve data and plot three different charts.
    """

    try:
        env_path = "../../DS01/ex00/.env"
        db_config = load_env_vars(env_path)
        conn, cur = connect_db(db_config)
        print("Select the table you want to take data from: ")
        table = select_table(cur)
        df = table_to_dataframe(cur, table)
        plot_line_chart(df)
        plot_histogram_chart(df)
        plot_area_chart(df)
    finally:
        cur.close()
        conn.close()
        print("Database connection closed.") 


if __name__ == "__main__":
    main()