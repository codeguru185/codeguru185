import psycopg2
import smtplib
from email.mime.text import MIMEText

def check_replication_status():
    try:
        # Connect to the Greenplum database
        conn = psycopg2.connect(
            host="your_host",
            database="your_database",
            user="your_user",
            password="your_password"
        )
        
        cursor = conn.cursor()
        
        # Check replication status
        cursor.execute("SELECT application_name, state FROM pg_stat_replication;")
        replication_status = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Process replication status
        for row in replication_status:
            application_name = row[0]
            state = row[1]
            
            if state != "streaming":
                send_alert_email(application_name, state)
        
    except Exception as e:
        send_alert_email("Replication Check Failed", str(e))

def send_alert_email(application_name, replication_state):
    from_address = "your_email@example.com"
    to_address = "recipient@example.com"
    smtp_server = "smtp.example.com"
    smtp_port = 587
    smtp_username = "your_smtp_username"
    smtp_password = "your_smtp_password"
    
    subject = "Greenplum Replication Alert"
    
    message = f"Replication issue detected for application: {application_name}\n"
    message += f"Replication State: {replication_state}\n"
    
    msg = MIMEText(message)
    msg["Subject"] = subject
    msg["From"] = from_address
    msg["To"] = to_address
    
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(from_address, to_address, msg.as_string())
    server.quit()

if __name__ == "__main__":
    check_replication_status()
