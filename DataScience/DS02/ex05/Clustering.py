import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import numpy as np

import sys
import os
sys.path.append("../../DS01/ex01")
from customers_table import load_env_vars, connect_db
sys.path.append("../../DS01/ex02")
from remove_duplicates import select_table
sys.path.append("../ex04")
from elbow import table_to_dataframe


def optimal_clusters(X_scaled):
    """
    Determine the optimal number of clusters using the Elbow Method.

    This function calculates the inertia (within-cluster sum of squares)
    for k values ranging from 1 to 9, and then looks for the "elbow point"
    where the inertia stops decreasing significantly.
    """
    inertia = []
    K = range(1, 10)
    for k in K:
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(X_scaled)
        inertia.append(kmeans.inertia_)

    deltas = np.diff(inertia)
    k_opt = np.argmin(deltas < (0.1 * deltas[0])) + 1

    return max(k_opt, 4)


def assign_loyalty_groups(user_stats, X_scaled, k_opt):
    """
    Assigns customers to loyalty groups based on purchase behavior.
    """
    # Step 1: inactive and new customers
    inactive_mask = (user_stats['num_purchases'] > 0) & (user_stats['days_since_last'] >= 90)
    new_mask = (user_stats['num_purchases'] > 0) & (user_stats['days_since_last'] <= 7)

    user_stats['loyalty_group'] = None
    user_stats.loc[inactive_mask, 'loyalty_group'] = 'inactive'
    user_stats.loc[new_mask, 'loyalty_group'] = 'new customer'

    # Step 2: remaining customers (to cluster)
    mask_remaining = ~(inactive_mask | new_mask)
    remaining = user_stats[mask_remaining].copy()
    X_remaining = X_scaled[mask_remaining]

    if len(remaining) > 0 and k_opt > 2:
        kmeans = KMeans(n_clusters=k_opt - 2, random_state=42)
        labels = kmeans.fit_predict(X_remaining)

        remaining['cluster'] = labels

        # Order clusters by mean total_spent
        order = remaining.groupby('cluster')['total_spent'].mean().sort_values().index.tolist()

        # Loyalty group names
        names = ['silver', 'gold']
        if (k_opt - 2) > 2:
            extra = [f'platinum{i+1}' for i in range((k_opt - 2) - 2)]
            names = names + extra

        # Map cluster → group
        cluster_to_group = {order[i]: names[i] for i in range(len(order))}
        remaining['loyalty_group'] = remaining['cluster'].map(cluster_to_group)

        # Put back into main df
        user_stats.loc[mask_remaining, 'loyalty_group'] = remaining['loyalty_group']

    return user_stats

def plot_rfm_groups(user_stats):
    """
    Plot a bubble chart of loyalty groups: median Recency vs median Frequency.
    
    Parameters
    ----------
    user_stats : pd.DataFrame
        Must contain 'loyalty_group', 'days_since_last', 'num_purchases'.
    """
    summary = user_stats.groupby('loyalty_group').agg(
        median_recency=('days_since_last', 'median'),
        median_frequency=('num_purchases', 'median'),
        count=('user_id', 'size')
    ).reset_index()

    # Define colors for each group
    color_map = {
        'inactive': '#999999',
        'new customer': '#1f77b4',
        'silver': '#c0c0c0',
        'gold': '#ffd700',
        'platinum': '#e5e4e2'
    }

    max_size = 2000  # tamaño máximo de burbuja
    summary['size'] = summary['count'] / summary['count'].max() * max_size
    # Plot
    plt.figure(figsize=(8,6))
    for _, row in summary.iterrows():
        plt.scatter(
            row['median_recency'],
            row['median_frequency'],
            s=row['size'],  # bubble size proportional to number of users
            color=color_map.get(row['loyalty_group'], 'skyblue'),
            alpha=0.6,
            label=row['loyalty_group']
        )

    plt.xlabel("Median Recency (days since last purchase)")
    plt.ylabel("Median Frequency (# of purchases)")
    plt.title("Customer Loyalty Groups: Recency vs Frequency")
    plt.legend(title="Loyalty Group")
    plt.grid(True)
    plt.show()

def plot_loyalty_bars(user_stats):
    """
    Plot a horizontal bar chart showing the number of customers
    in each loyalty group, with a distinct color per group.
    """
    counts = user_stats['loyalty_group'].value_counts()

    # Define base colors for known groups
    color_map = {
        'inactive': '#999999',      # grey
        'new customer': '#1f77b4',  # blue
        'silver': '#c0c0c0',        # silver
        'gold': '#ffd700',          # gold
        'platinum': '#e5e4e2'       # platinum
    }

    # For dynamic platinum groups (platinum1, platinum2, ...)
    colors = []
    for group in counts.index:
        if group in color_map:
            colors.append(color_map[group])
        elif group.startswith("platinum"):
            colors.append('#e5e4e2')  # same platinum color
        else:
            colors.append('skyblue')  # fallback

    plt.figure(figsize=(8, 5))
    plt.barh(counts.index, counts.values, color=colors)
    plt.xlabel("Number of Customers")
    plt.ylabel("Customer Group")
    plt.title("Number of Customers per Loyalty Group")
    plt.tight_layout()
    plt.show()



def main():
    try:
        env_path = "../../DS01/ex00/.env"
        db_config = load_env_vars(env_path)
        conn, cur = connect_db(db_config)
        print("Select the table you want to take data from: ")
        table = select_table(cur)
        df = table_to_dataframe(cur, table)
        df['event_time'] = pd.to_datetime(df['event_time'])



        # purchases por usuario
        events_per_user = df[df['event_type'] == 'purchase'] \
                            .groupby('user_id').size().reset_index(name='num_purchases')

        # gasto total
        spent_per_user = df[df['event_type'] == 'purchase'] \
                            .groupby('user_id')['price'].sum().reset_index(name='total_spent')

        # última compra
        last_purchase = df[df['event_type'] == 'purchase'] \
                            .groupby('user_id')['event_time'].max().reset_index()
        last_purchase['event_time'] = last_purchase['event_time'].dt.tz_localize(None)
        today = pd.Timestamp("2023-02-1")
        last_purchase['days_since_last'] = (today - last_purchase['event_time']).dt.days

        # merge con ALL users
        user_stats = events_per_user\
            .merge(spent_per_user, on='user_id', how='left') \
            .merge(last_purchase[['user_id', 'days_since_last']], on='user_id', how='left') \
            .fillna({'num_purchases': 0, 'total_spent': 0, 'days_since_last': 9999})

        # features para clustering
        X = user_stats[['num_purchases', 'total_spent', 'days_since_last']]
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        k_opt = optimal_clusters(X_scaled)
        user_stats = assign_loyalty_groups(user_stats, X_scaled, k_opt)

        plot_loyalty_bars(user_stats)
        plot_rfm_groups(user_stats)

    finally:
        cur.close()
        conn.close()
        print("Database connection closed.")



if __name__ == "__main__":
    main()