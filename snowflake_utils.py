import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def create_table_from_csv(conn_info, csv_path, table_name=None):
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=conn_info['user'],
        password=conn_info['password'],
        role=conn_info['role'], 
        account=conn_info['account'],
        warehouse=conn_info['warehouse'],
        database=conn_info['database'],
        schema=conn_info['schema']
    )
    # Read CSV file with pandas
    df = pd.read_csv(csv_path)
    # If table_name is not provided, use the base name of the CSV file
    if table_name is None:
        table_name = os.path.splitext(os.path.basename(csv_path))[0]
    # Use the write_pandas function to write the DataFrame to Snowflake
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name, database=conn_info['database'], schema=conn_info['schema'], auto_create_table=True)
    # Close the connection
    conn.close()
    print(f'Success: {success}, Chunks: {nchunks}, Rows: {nrows}')
    return success, nchunks, nrows

def remove_duplicates(conn_info, table_name):
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=conn_info['user'],
        password=conn_info['password'],
        role=conn_info['role'], 
        account=conn_info['account'],
        warehouse=conn_info['warehouse'],
        database=conn_info['database'],
        schema=conn_info['schema']
    )

    # Create a cursor object
    cur = conn.cursor()

    try:
        # Create a temporary table with distinct rows
        cur.execute(f"""
            CREATE OR REPLACE TABLE "{conn_info['database']}".{conn_info['schema']}."{table_name}_temp" AS
            SELECT DISTINCT * FROM "{conn_info['database']}".{conn_info['schema']}."{table_name}";
        """)
        print(f"Temporary table {table_name}_temp created successfully.")

        # Drop the original table
        cur.execute(f'DROP TABLE "{conn_info["database"]}"."{conn_info["schema"]}"."{table_name}";')
        print(f"Original table {table_name} dropped successfully.")

        # Rename the temporary table to the original table name
        cur.execute(f'ALTER TABLE "{conn_info["database"]}"."{conn_info["schema"]}"."{table_name}_temp" RENAME TO "{table_name}";')
        print(f"Temporary table renamed to {table_name} successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()