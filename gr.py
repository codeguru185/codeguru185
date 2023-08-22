import psycopg2
from psycopg2 import sql

# Database connection parameters
db_params = {
    'dbname': 'your_database',
    'user': 'your_user',
    'password': 'your_password',
    'host': 'your_host',
    'port': 'your_port'
}

def execute_grant_statement(cursor, role, schema, permission):
    grant_statement = sql.SQL("GRANT {} ON SCHEMA {} TO {}").format(
        sql.Identifier(permission),
        sql.Identifier(schema),
        sql.Identifier(role)
    )
    cursor.execute(grant_statement)

def main():
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Read role, schema, permission from config table
        cursor.execute("SELECT role_name, schema_name, permission FROM config_table")
        config_entries = cursor.fetchall()

        for entry in config_entries:
            role_name, schema_name, permission = entry
            try:
                execute_grant_statement(cursor, role_name, schema_name, permission)
                print(f"Granted {permission} permission to {role_name} on schema {schema_name}")
            except Exception as e:
                print(f"Error granting {permission} permission to {role_name} on schema {schema_name}: {e}")
        
        # Commit changes
        conn.commit()
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        # Close the connection
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
