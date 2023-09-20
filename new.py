import pyodbc
import psycopg2

# SQL Server database connection parameters
sql_server_conn_str = (
    "Driver={SQL Server Native Client 11.0};"
    "Server=SQL_SERVER_HOST;"
    "Database=SQL_SERVER_DB_NAME;"
    "UID=SQL_SERVER_USERNAME;"
    "PWD=SQL_SERVER_PASSWORD;"
)

# Greenplum database connection parameters
greenplum_conn_str = (
    "dbname=GREENPLUM_DB_NAME "
    "user=GREENPLUM_USERNAME "
    "password=GREENPLUM_PASSWORD "
    "host=GREENPLUM_HOST "
    "port=GREENPLUM_PORT"
)

# SQL query to select data from SQL Server table
sql_query = "SELECT * FROM SQL_SERVER_TABLE_NAME"

try:
    # Connect to SQL Server
    sql_server_conn = pyodbc.connect(sql_server_conn_str)
    sql_cursor = sql_server_conn.cursor()

    # Execute the SQL query
    sql_cursor.execute(sql_query)

    # Fetch data from SQL Server
    sql_data = sql_cursor.fetchall()

    # Connect to Greenplum
    greenplum_conn = psycopg2.connect(greenplum_conn_str)
    greenplum_cursor = greenplum_conn.cursor()

    # Greenplum table name to load data into
    greenplum_table_name = "GREENPLUM_TABLE_NAME"

    # Prepare the INSERT statement for Greenplum
    insert_query = f"INSERT INTO {greenplum_table_name} VALUES %s"

    # Load data into Greenplum
    psycopg2.extras.execute_values(
        greenplum_cursor,
        insert_query,
        sql_data,  # Data fetched from SQL Server
        template=None,
        page_size=1000,  # Adjust as needed
    )

    # Commit the transaction
    greenplum_conn.commit()

    print("Data loaded successfully into Greenplum.")

except Exception as e:
    print(f"Error: {str(e)}")

finally:
    # Close database connections
    if sql_server_conn:
        sql_server_conn.close()
    if greenplum_conn:
        greenplum_conn.close()
