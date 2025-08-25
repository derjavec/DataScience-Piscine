import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

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
    """

    cur.execute(query)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=['event_time', 'event_type', 'price', 'user_id'])

    if df.empty:
        raise ValueError(f"No events found in table '{table}'.")

    return df

def plot_elbow(X):
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    inertia = []
    for k in range(1, 10):
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(X_scaled)
        inertia.append(kmeans.inertia_)

    plt.figure(figsize=(6, 4))
    plt.plot(range(1, 10), inertia)
    plt.xlabel("Number of clusters")
    plt.ylabel("Inertia")
    plt.title("Elbow Method for optimal k")
    plt.show()


def main():
    """
    1. Load database credentials.
    2. Connect to PostgreSQL.
    3. Let the user select a table and column.
    4. Retrieve data and plot an elbow chart taking in account events per user and money spent per user.
    """

    try:
        env_path = "../../DS01/ex00/.env"
        db_config = load_env_vars(env_path)
        conn, cur = connect_db(db_config)
        print("Select the table you want to take data from: ")
        table = select_table(cur)
        df = table_to_dataframe(cur, table)
        df['event_time'] = pd.to_datetime(df['event_time'])
        
        events_per_user = df[df['event_type'] == 'purchase'].groupby('user_id').size().reset_index(name='num_purchases')
        spent_per_user = df[df['event_type'] == 'purchase'].groupby(df['user_id'])['price'].sum().reset_index(name='total_spent')
        last_purchase = df[df['event_type'] == 'purchase'].groupby('user_id')['event_time'].max().reset_index()
        last_purchase['event_time'] = last_purchase['event_time'].dt.tz_localize(None)
        last_purchase['days_since_last'] = (pd.Timestamp.now() - last_purchase['event_time']).dt.days


        user_stats = events_per_user.merge(spent_per_user, on='user_id', how='outer')\
                                .merge(last_purchase[['user_id','days_since_last']], on='user_id', how='outer')\
                                .fillna(0)
        X = user_stats[['num_purchases', 'total_spent', 'days_since_last']]

        plot_elbow(X)
       
        

    finally:
        cur.close()
        conn.close()
        print("Database connection closed.") 


if __name__ == "__main__":
    main()