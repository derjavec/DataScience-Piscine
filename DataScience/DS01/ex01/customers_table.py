from dotenv import load_dotenv
import psycopg2
import pandas as pd
import os


def load_env_vars():
    """
    Load environment variables from a .env
    file and return database configuration.
    """
    load_dotenv()
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


def join_data(cur):
    """
    Joins all the tables which names begin with data_202
    """
    print("Joining tables... please wait")
    cur.execute("""
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public'
      AND tablename LIKE 'data_202%';
    """)

    tables = [row[0] for row in cur.fetchall()]

    if tables:
        union_query = "\nUNION ALL\n".\
                        join(f"SELECT * FROM {t}" for t in tables)
        create_customers_sql = f"""
            DROP TABLE IF EXISTS customers;
            CREATE TABLE customers AS
            {union_query};
        """
        cur.execute(create_customers_sql)
    print(f"tables : {tables} joined succesfully")

def main():
    """
    Main function to process all CSV files
    in a folder and import them into PostgreSQL.
    """
    CSV_FOLDER = "../customer"
    db_config = load_env_vars()
    conn, cur = connect_db(db_config)

    try:
        for filename in os.listdir(CSV_FOLDER):
            process_csv_file(cur, CSV_FOLDER, filename)
        join_data(cur)
    finally:
        cur.close()
        conn.close()
        print("All CSV files processed and database connection closed.")


if __name__ == "__main__":
    main()

# SELECT COUNT(*) AS total_rows FROM customers;
