from dotenv import load_dotenv
import psycopg2
import pandas as pd
import os
from tqdm import tqdm
import sys
sys.path.append("../ex01")
from customers_table import load_env_vars, connect_db, get_pg_type, create_table_from_df, insert_csv_data, process_csv_file, select_folder
sys.path.append("../ex02")
from remove_duplicates import select_table, get_columns


def add_missing_columns_from_items(cur, table1, table2):
    """
    Add missing columns from table2 to table1 using only psycopg2 cursor.
    Avoids pandas read_sql warning.
    """
    print("Adding missing columns from new table")

    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s;
    """

    cur.execute(query, (table1,))
    cols1 = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute(query, (table2,))
    cols2 = {row[0]: row[1] for row in cur.fetchall()}

    missing_cols = [col for col in cols2 if col not in cols1]

    if not missing_cols:
        print("No missing columns found.")
        return []

    alter_parts = [f"ADD COLUMN {col} {map_pg_type(cols2[col])}" for col in missing_cols]

    alter_sql = f"ALTER TABLE {table1} {', '.join(alter_parts)};"
    cur.execute(alter_sql)

    print(f"Added missing columns: {missing_cols}")
    return missing_cols


def map_pg_type(pg_type_str):
    """
    Map PostgreSQL information_schema data_type to SQL column type string.
    (Puedes expandir este mapeo según tus necesidades)
    """
    mapping = {
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "text": "TEXT",
        "character varying": "VARCHAR",
        "double precision": "DOUBLE PRECISION",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp without time zone": "TIMESTAMP",
    }
    return mapping.get(pg_type_str, "TEXT")


def get_matching_keys(cur, table1, table2, common_col):
    """
    Return a list of keys from table1 that also exist in table2.
    """
    cur.execute(f"""
        SELECT DISTINCT c.{common_col}
        FROM {table1} c
        JOIN {table2} i ON c.{common_col} = i.{common_col}
        WHERE c.{common_col} IS NOT NULL;
    """)
    return [row[0] for row in cur.fetchall()]


def update_in_batches(cur, keys, table1, table2, common_col, missing_cols, batch_size=1000):
    """
    Update missing columns in table1 from table2 for matching keys, in batches.
    """
    to_set = ", ".join(f"{col} = i.{col}" for col in missing_cols)

    print(f"Updating only {len(keys)} matching rows (in batches of {batch_size})...")
    for i in tqdm(range(0, len(keys), batch_size), desc="Updating rows", unit="batch"):
        batch_keys = keys[i:i + batch_size]
        keys_str = ",".join(f"'{k}'" for k in batch_keys if k is not None)
        if not keys_str:
            continue

        update_sql = f"""
        UPDATE {table1} AS c
        SET {to_set}
        FROM {table2} AS i
        WHERE c.{common_col} = i.{common_col}
          AND c.{common_col} IN ({keys_str});
        """
        cur.execute(update_sql)


def look_up(cur, missing_cols, common_col, table1, table2, batch_size=1000):
    """
    Orchestrates the lookup process:
    - Choose the common key column
    - Get only matching keys
    - Update table1 from table2 in batches
    """
    if not missing_cols or not common_col:
        print("✅ No lookup needed — missing columns or common columns not found.")
        return

    print(f"🔍 Searching values for columns: {missing_cols}")
    print(f"Using key column: {common_col}")

    keys = get_matching_keys(cur, table1, table2, common_col)
    if not keys:
        print("✅ No matching keys found — nothing to update.")
        return

    update_in_batches(cur, keys, table1, table2, common_col, missing_cols, batch_size)
    print("✅ Lookup completed and data inserted.")


def keep_most_complete_per_key(cur, table, key_col):
    """
    Keep only the row with the most non-null values per key column.
    Deletes all other rows for that product_id.
    """
    score_cols = " + ".join(f"(CASE WHEN {col} IS NOT NULL THEN 1 ELSE 0 END)" 
                            for col in get_columns(cur, table) if col != key_col)

    delete_sql = f"""
    DELETE FROM {table} t
    USING (
        SELECT ctid
        FROM (
            SELECT ctid,
                   {key_col},
                   ROW_NUMBER() OVER (
                       PARTITION BY {key_col}
                       ORDER BY {score_cols} DESC
                   ) AS rn
            FROM {table}
        ) sub
        WHERE rn > 1
    ) to_delete
    WHERE t.ctid = to_delete.ctid;
    """

    cur.execute(delete_sql)
    print(f"✅ Table {table} cleaned: only the most complete row per {key_col} kept.")


def choose_column(common_cols, instruction: str):
    """
    Let the user choose a column from a set of columns.
    Converts the set to a sorted list to ensure consistent ordering.
    """
    cols_list = sorted(list(common_cols))

    if len(cols_list) == 1:
        return cols_list[0]

    print("Multiple common columns found:")
    for idx, col in enumerate(cols_list, start=1):
        print(f"{idx}: {col}")

    while True:
        try:
            choice = int(input(instruction))
            if 1 <= choice <= len(cols_list):
                return cols_list[choice - 1]
            else:
                print(f"Please enter a number between 1 and {len(cols_list)}")
        except ValueError:
            print("Please enter a number")


def find_common_key(cur, table1, table2):
    """
    Find common columns between two tables.
    """
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s;
    """
    
    cur.execute(query, (table1,))
    cols1 = {row[0] for row in cur.fetchall()}

    cur.execute(query, (table2,))
    cols2 = {row[0] for row in cur.fetchall()}

    common_cols = cols1 & cols2

    if not common_cols:
        raise ValueError(f"No common columns between {table1} and {table2}")

    return common_cols

def select_and_proccess_tables(cur, csv_folder):
    """
    Process CSV files in the folder and let the user select one table.
    """
    table_names = []
    for filename in os.listdir(csv_folder):
        tname = process_csv_file(cur, csv_folder, filename)
        if tname:
            table_names.append(tname)

    if not table_names:
        print("⚠ No tables found in the folder.")
        return None, []

    if len(table_names) == 1:
        table2 = table_names[0]
    else:
        print("Multiple tables found:")
        for i, tname in enumerate(table_names, start=1):
            print(f"Table {i}: {tname}")

        while True:
            try:
                choice = int(input(f"Choose the table to use (1-{len(table_names)}): "))
                if 1 <= choice <= len(table_names):
                    table2 = table_names[choice - 1]
                    break
                else:
                    print(f"Please enter a number between 1 and {len(table_names)}.")
            except ValueError:
                print("Please enter a valid number.")

    return table2


def main():
    DATA_FOLDER = "../data"
    print("Select the folder with the CSV files you want to merge and match:")
    CSV_FOLDER = select_folder(DATA_FOLDER)
    if CSV_FOLDER is None:
        print("Please create a folder named 'data' and put your CSV files inside, then recompile.")
        return
        
    env_path = "../ex00/.env"
    db_config = load_env_vars(env_path)
    conn, cur = connect_db(db_config)
    print("select the table where you want to add the columns:")
    table1 = select_table(cur)

    try:
        table2 = select_and_proccess_tables(cur, CSV_FOLDER)
        
        if not table2:
            print("No table selected. Exiting.")
            return

        common_cols = find_common_key(cur, table1, table2)
        common_col = choose_column(common_cols, "Choose the column to use as key for lookup: ")
        keep_most_complete_per_key(cur, table2, common_col)
        missing_cols = add_missing_columns_from_items(cur, table1, table2)
        look_up(cur, missing_cols, common_col, table1, table2)

    finally:
        cur.close()
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
