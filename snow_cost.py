import snowflake.connector
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Snowflake connection parameters
account = 'your_account'
user = 'your_user'
password = 'your_password'
warehouse = 'your_warehouse'
database = 'your_database'
schema = 'your_schema'

def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        logging.info("Connected to Snowflake successfully.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        raise

def execute_query(cursor, query):
    try:
        cursor.execute(query)
        logging.info("Query executed successfully.")
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise

def main():
    conn = None
    try:
        conn = connect_to_snowflake()
        cur = conn.cursor()

        # Query to retrieve cost-related information at Virtual Warehouse (VDW) level
        query_vdw = """
        SELECT WAREHOUSE_NAME, SUM(CREDIT_USAGE) AS total_cost
        FROM "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_METERING_HISTORY"
        GROUP BY WAREHOUSE_NAME
        """

        execute_query(cur, query_vdw)

        print("Cost at Virtual Warehouse level:")
        print("----------------------------------")
        for row in cur.fetchall():
            print(f"Warehouse: {row[0]}, Total Cost: ${row[1]:.2f}")
        print()

        # ... Similar queries for User level and Storage usage ...

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Connection closed.")

if __name__ == "__main__":
    main()
