import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def create_table_from_csv(conn_info, table_name, csv_path):
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
    # Create a cursor
    cursor = conn.cursor()

    # Set the database for the session
    cursor.execute(f"USE DATABASE {conn_info['database']}")

    # Read CSV file with pandas
    df = pd.read_csv(csv_path)

    # Use the write_pandas function to write the DataFrame to Snowflake
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name, database=conn_info['database'], schema=conn_info['schema'], auto_create_table=True)

    # Close the connection
    conn.close()

    return success, nchunks, nrows