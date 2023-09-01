import psycopg2
from psycopg2 import sql

# Database connection parameters
db_params = {
    'host': 'your_host',
    'port': 'your_port',
    'database': 'your_database',
    'user': 'your_user',
    'password': 'your_password'
}

try:
    # Connect to Greenplum
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Step 1: Read data from t_prev
    cursor.execute("SELECT * FROM t_prev")
    t_prev_data = cursor.fetchall()

    # Step 2: Read data from t_curr
    cursor.execute("SELECT * FROM t_curr")
    t_curr_data = cursor.fetchall()

    # Step 3: Join the tables on id
    joined_data = []
    for prev_row in t_prev_data:
        for curr_row in t_curr_data:
            if prev_row[0] == curr_row[0]:  # Assuming id is the first column
                # Step 4: Capture CDC info
                cdc_info = []
                for i in range(1, len(prev_row)):  # Assuming the first column is id
                    if prev_row[i] != curr_row[i]:
                        cdc_info.append((prev_row[0], i, prev_row[i], curr_row[i]))
                joined_data.extend(cdc_info)
    
    # Step 5: Insert CDC info into t_chg with insert_ts
    if joined_data:
        insert_query = sql.SQL("INSERT INTO t_chg (id, field, old_value, new_value, insert_ts) VALUES {}").format(
            sql.SQL(',').join(map(sql.Literal, joined_data))
        )
        cursor.execute(insert_query)
        conn.commit()

    # Step 6: Capture current timestamp for newly created records
    new_ids = set(curr_row[0] for curr_row in t_curr_data) - set(prev_row[0] for prev_row in t_prev_data)
    if new_ids:
        for new_id in new_ids:
            cursor.execute("INSERT INTO t_chg (id, field, old_value, new_value, insert_ts) VALUES (%s, NULL, NULL, NULL, current_timestamp)", (new_id,))
        conn.commit()

except psycopg2.Error as e:
    print(f"Error: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
