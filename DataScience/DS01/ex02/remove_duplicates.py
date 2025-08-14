from dotenv import load_dotenv
import psycopg2
import os
import pandas as pd
from tqdm import tqdm
import sys
sys.path.append("../ex01")
from customers_table import load_env_vars, connect_db


def get_columns(cur, table):
    """
    Get all column names from a table (excluding ctid) using a psycopg2 cursor.
    """
    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
    """
    cur.execute(query, (table,))
    return [row[0] for row in cur.fetchall()]


def delete_exact_duplicates(cur, table1):
    """
    Counts and deletes by batchs all the exact duplicated rows
    """
    print("Counting duplicated rows... please wait")

    columns = get_columns(cur, table1)
    partition_by = ", ".join(columns)

    cur.execute(f"""
        WITH duplicates AS (
            SELECT ctid,
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_by}
                    ORDER BY ctid
                ) AS rn
            FROM {table1}
        )
        SELECT COUNT(*) FROM duplicates WHERE rn > 1;
    """)
    total_exact = cur.fetchone()[0]
    print(f"Total exact duplicates to delete: {total_exact}")

    if total_exact == 0:
        print("No exact duplicates found.")
        return
    print("Deleting exact duplicates...")
    cur.execute(f"""
        WITH duplicates AS (
            SELECT ctid,
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_by}
                    ORDER BY ctid
                ) AS rn
            FROM {table1}
        )
        SELECT ctid FROM duplicates WHERE rn > 1;
    """)
    ctids_exact = [row[0] for row in cur.fetchall()]

    batch_size = 1000
    for i in tqdm(range(0, total_exact, batch_size)):
        batch = ctids_exact[i:i+batch_size]
        ctids_str = ','.join(f"'{ctid}'" for ctid in batch)
        delete_query = f"DELETE FROM {table1} WHERE ctid IN ({ctids_str});"
        cur.execute(delete_query)

    print("Exact duplicates removed.")


def delete_temporal_duplicated(cur, table1):
    """
    Counts and deletes by batchs all the temporal duplicated rows
    """
    print("Counting temporal duplicated rows... please wait")
    columns = get_columns(cur, table1)
    partition_by = ", ".join(columns)
    partition_cols = [col for col in columns if col != "event_time"]
    partition_by = ", ".join(partition_cols)

    cur.execute(f"""
        WITH ranked_events AS (
            SELECT
                ctid,
                event_time,
                {partition_by},
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_by}
                    ORDER BY event_time::timestamptz
                ) AS rn
            FROM {table1}
        ),
        to_delete AS (
            SELECT c1.ctid
            FROM ranked_events c1
            JOIN ranked_events c2
            ON {' AND '.join([f'c1.{col} = c2.{col}' for col in partition_cols])}
            AND c1.rn = c2.rn + 1
            AND c1.event_time::timestamptz <= c2.event_time::timestamptz
                + INTERVAL '1 second'
        )
        SELECT ctid FROM to_delete;
        """)

    ctids_to_delete = [row[0] for row in cur.fetchall()]
    total = len(ctids_to_delete)
    print(f"Total temporal duplicate rows to delete: {total}")

    batch_size = 1000
    for i in tqdm(range(0, total, batch_size)):
        batch = ctids_to_delete[i:i+batch_size]
        ctids_str = ','.join(f"'{ctid}'" for ctid in batch)
        delete_query = f"DELETE FROM customers WHERE ctid IN ({ctids_str});"
        cur.execute(delete_query)

    print("Temporal duplicates removed.")


def select_table(cur):
    """
    List all tables in the public schema and let the user select one.
    Returns the chosen table name.
    """
    cur.execute("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public';
    """) 
    tables = [row[0] for row in cur.fetchall()]
    if not tables:
        print("No tables found in the database.")
        return None
    print("Select the table to remove duplicates from:")
    for i, table in enumerate(tables, start = 1):
        print(f"Table {i} : {table}")
    
    while True:
        choice = input(f"Select the table by number (1-{len(tables)}): ")
        try:
            choice = int(choice)
            if 1 <= choice <= len(tables):
                return tables[choice - 1]
            else:
                print(f"Please enter a number between 1 and {len(tables)}")
        except ValueError:
            print("Please enter a valid number.")


def main():
    """
    Main function to process all CSV files
    in a folder and import them into PostgreSQL.
    """
    env_path = "../ex00/.env"
    db_config = load_env_vars(env_path)
    conn, cur = connect_db(db_config)
    table1 = select_table(cur)
    print(f"You selected: {table1}")
    try:
        delete_exact_duplicates(cur,  table1)
        delete_temporal_duplicated(cur, table1)
    finally:
        cur.close()
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()

# SELECT COUNT(*) AS duplicated_groups
# FROM (
#     SELECT event_time, event_type, product_id, price, user_id, user_session
#     FROM customers
#     GROUP BY event_time, event_type, product_id, price, user_id, user_session
#     HAVING COUNT(*) > 1
# ) AS sub;
