import psycopg2

# Database connection details
db_config = {
    "host": "your_host",
    "database": "your_database",
    "user": "your_user",
    "password": "your_password"
}

def check_table_bloat():
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT schemaname, tablename, pg_size_pretty(table_size) AS table_size,
                   pg_size_pretty(index_size) AS index_size,
                   pg_size_pretty(total_size) AS total_size
            FROM (
                SELECT schemaname, tablename,
                       pg_total_relation_size('"' || schemaname || '"."' || tablename || '"') AS total_size,
                       pg_relation_size('"' || schemaname || '"."' || tablename || '"') AS table_size,
                       pg_indexes_size('"' || schemaname || '"."' || tablename || '"') AS index_size
                FROM pg_tables
            ) AS sizes
            WHERE total_size > 10 * table_size;  -- Adjust the threshold as needed
        """)
        
        bloat_info = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if bloat_info:
            print("Tables with Excessive Bloat:")
            for row in bloat_info:
                print(f"Schema: {row[0]}, Table: {row[1]}")
                print(f"Table Size: {row[2]}, Index Size: {row[3]}, Total Size: {row[4]}\n")
        else:
            print("No excessive table bloat detected.")
        
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    check_table_bloat()
