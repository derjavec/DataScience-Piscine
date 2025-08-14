from dotenv import load_dotenv
import psycopg2
import pandas as pd
import os
import sys
sys.path.append("../ex03")
from automatic_table import load_env_vars, connect_db, process_csv_file

def main():
    """
    Main function to process all CSV files
    in a folder and import them into PostgreSQL.
    """
    CSV_FOLDER = "../data//item"
    env_path = "../ex01/.env"
    db_config = load_env_vars(env_path)
    conn, cur = connect_db(db_config)

    try:
        for filename in os.listdir(CSV_FOLDER):
            process_csv_file(cur, CSV_FOLDER, filename)
    finally:
        cur.close()
        conn.close()
        print("All CSV files processed and database connection closed.")


if __name__ == "__main__":
    main()
