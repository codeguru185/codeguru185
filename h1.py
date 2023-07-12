import psycopg2
from pyhive import hive

# Establish connection to Hive
hive_connection = hive.Connection(host='your_hive_host', port=10000, username='your_username')

# Establish connection to XYZ
xyz_connection = psycopg2.connect(host='your_xyz_host', port=5432, dbname='your_dbname', user='your_username', password='your_password')

# Task 1: Read distinct year_month from Hive table p1
hive_cursor = hive_connection.cursor()
hive_cursor.execute("SELECT DISTINCT year_month FROM p1")
hive_partitions_hive = hive_cursor.fetchall()
hive_cursor.close()

# Task 2: Read distinct year_month from XYZ table p2
xyz_cursor = xyz_connection.cursor()
xyz_cursor.execute("SELECT DISTINCT year_month FROM p2")
xyz_partitions = xyz_cursor.fetchall()
xyz_cursor.close()

# Task 3: Find partitions not present in XYZ table p2
new_partitions = [partition for partition in hive_partitions_hive if partition not in xyz_partitions]

# Task 4: Insert into Hive table ht2 for each new partition
hive_cursor = hive_connection.cursor()
for partition in new_partitions:
    year_month = partition[0]
    query = f"INSERT INTO ht2 SELECT * FROM ht1 WHERE year_month = '{year_month}'"
    hive_cursor.execute(query)

# Commit the changes in Hive
hive_connection.commit()

# Close the connections
hive_connection.close()
xyz_connection.close()
