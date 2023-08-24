import psycopg2
import time

# Database connection details
db_config = {
    "host": "your_host",
    "database": "your_database",
    "user": "your_user",
    "password": "your_password"
}

def monitor_database_health():
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute("SELECT datname, numbackends, xact_commit, xact_rollback, deadlocks FROM pg_stat_database;")
        databases = cursor.fetchall()
        
        # Analyze database health
        for database in databases:
            datname = database[0]
            numbackends = database[1]
            xact_commit = database[2]
            xact_rollback = database[3]
            deadlocks = database[4]
            
            # Perform analysis and alert if needed
            if numbackends > 100:
                send_alert_email(datname, "High connection count", f"Active connections: {numbackends}")
            
            # Add more health checks as needed
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        send_alert_email("Database Health Monitoring Error", str(e))

def send_alert_email(database_name, issue, details):
    # Implement the email sending logic here

if __name__ == "__main__":
    while True:
        monitor_database_health()
        time.sleep(3600)  # Run the monitoring process every hour
