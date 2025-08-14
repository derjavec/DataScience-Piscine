from dotenv import load_dotenv
import psycopg2
import pandas as pd
import os


def load_env_vars(env_path: str):
    """
    Load environment variables from a .env
    file and return database configuration.
    """
    load_dotenv(dotenv_path=env_path)
    return {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT")
    }


def connect_db(db_config):
    """
    Establish a connection to the PostgreSQL
    database using the provided config.
    """
    conn = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )
    conn.autocommit = True
    return conn, conn.cursor()


def get_pg_type(dtype):
    """
    Map a pandas data type to an
    appropriate PostgreSQL data type.
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


def create_table_from_df(cursor, table_name, df):
    """
    Create a PostgreSQL table based on the DataFrame's columns and types.

    Drops the table if it exists before creating a new one.
    """
    columns_with_types = [f"{col} {get_pg_type(df[col].dtype)}"
                          for col in df.columns]
    create_table_sql = (
        f"DROP TABLE IF EXISTS {table_name};\n"
        f"CREATE TABLE {table_name} (\n  "
        + ",\n  ".join(columns_with_types)
        + "\n);"
    )
    cursor.execute(create_table_sql)
    print(f"Table '{table_name}' created successfully.")


def insert_csv_data(cursor, table_name, csv_path):
    """
    Insert CSV data into the specified PostgreSQL table using COPY.
    """
    with open(csv_path, "r") as f:
        cursor.copy_expert(f"COPY {table_name} FROM STDIN CSV HEADER", f)
    print(f"Data successfully inserted into '{table_name}'.")


def process_csv_file(cursor, folder_path, filename):
    """
    Process a single CSV file: create corresponding table and insert data.
    """
    if not filename.endswith(".csv"):
        return
    table_name = os.path.splitext(filename)[0]
    csv_path = os.path.join(folder_path, filename)
    print(f"Processing file: {csv_path} ...")
    df = pd.read_csv(csv_path)
    create_table_from_df(cursor, table_name, df)
    insert_csv_data(cursor, table_name, csv_path)
    return table_name


def check_same_columns(cur, tables):
    """
    Check if all tables in the provided list have the same columns 
    (same names and same order).
    """
    if not tables:
        raise ValueError("⚠ No tables provided for column check.")

    # Get reference columns from the first table
    cur.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{tables[0]}'
        ORDER BY ordinal_position;
    """)
    reference_columns = [row[0] for row in cur.fetchall()]

    # Compare with the rest of the tables
    for t in tables[1:]:
        cur.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{t}'
            ORDER BY ordinal_position;
        """)
        cols = [row[0] for row in cur.fetchall()]
        if cols != reference_columns:
            raise ValueError(
                f"Column mismatch between '{tables[0]}' and '{t}':\n"
                f"{tables[0]}: {reference_columns}\n"
                f"{t}: {cols}"
            )

    return reference_columns


def join_data(cur, table1, tables):
    """
    Join given tables, 
    after verifying they have the same columns.
    """
    print("Joining tables... please wait")

    # Find tables with the given prefix

    if not tables:
        print("⚠ No tables found to join.")
        return

    # Check column consistency
    columns = check_same_columns(cur, tables)

    # Build the UNION ALL query
    union_query = "\nUNION ALL\n".join(f"SELECT * FROM {t}" for t in tables)
    create_customers_sql = f"""
        DROP TABLE IF EXISTS {table1};
        CREATE TABLE {table1} AS
        {union_query};
    """
    cur.execute(create_customers_sql)

    print(f"Tables {tables} joined successfully into '{table1}' with columns: {columns}")


def select_folder(DATA_FOLDER):
    if not os.path.exists(DATA_FOLDER):
        print(f"⚠ The folder '{DATA_FOLDER}' does not exist.")
        print("Please create a folder named 'data' and put your CSV files inside.")
        return None

    subfolders = [f for f in os.listdir(DATA_FOLDER) if os.path.isdir(os.path.join(DATA_FOLDER, f))]

    # Mostrar opciones al usuario
    print("Available folders:")
    for i, folder in enumerate(subfolders, start=1):
        print(f"{i}: {folder}")

    # Pedir al usuario que elija una
    while True:
        try:
            choice = int(input(f"Select a folder (1-{len(subfolders)}): "))
            if 1 <= choice <= len(subfolders):
                CSV_FOLDER = os.path.join(DATA_FOLDER, subfolders[choice - 1])
                return CSV_FOLDER
            else:
                print(f"Please enter a number between 1 and {len(subfolders)}")
        except ValueError:
            print("Please enter a number.")



def select_tables_to_join(cur, csv_folder):
    """
    Process all CSV files in a folder and let the user select which tables to join.
    """
    table_names = []
    for filename in os.listdir(csv_folder):
        tname = process_csv_file(cur, csv_folder, filename)
        if tname:
            table_names.append(tname)

    if not table_names:
        print("⚠ No tables found in the folder.")
        return []

    print("Please select the tables to join:")
    for i, tname in enumerate(table_names, start=1):
        print(f"Table {i}: {tname}")

    while True:
        selection = input(
            "Enter the numbers of the tables to join, separated by commas, or type 'all' to select all: "
        ).strip().lower()

        if selection == "all":
            return table_names

        try:
            indices = [int(x.strip()) - 1 for x in selection.split(",")]
            if all(0 <= idx < len(table_names) for idx in indices):
                return [table_names[idx] for idx in indices]
            else:
                print(f"Please enter numbers between 1 and {len(table_names)}.")
        except ValueError:
            print("Invalid input. Enter numbers separated by commas or 'all'.")


def main():
    """
    Main function to process all CSV files
    in a folder and import them into PostgreSQL.
    """
    DATA_FOLDER = "../data"
    env_path = "../ex00/.env"
    db_config = load_env_vars(env_path)
    conn, cur = connect_db(db_config)
    table1 = input("Enter the name for the new table to create: ")

    try:
        print("Select the folder containing the CSV files to join:")
        CSV_FOLDER = select_folder(DATA_FOLDER)
        print(f"You selected: {CSV_FOLDER}")
        selected_tables = select_tables_to_join(cur, CSV_FOLDER)
        print(f"Tables selected: {selected_tables}")
        join_data(cur, table1, selected_tables)
    finally:
        cur.close()
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()

# SELECT COUNT(*) AS total_rows FROM customers;
