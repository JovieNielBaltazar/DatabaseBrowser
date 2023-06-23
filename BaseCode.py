import csv
import mysql.connector
import os
from datetime import datetime

# MySQL database connection details
host = 'localhost'
user = 'root'
password = ''
database = 'db1'
port = 3306

# Table name to export to CSV
table_name = 'cocolife_profile'

# CSV file path to save the data
file_path = 'C:\\Users\\Joviel Niel Baltazar\\PycharmProjects\\DL-from-DB\\CSV\\'  # Use forward slashes instead

# Get the current date and time
current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

# Full path including the table name, date, and extension
saved_csvfile_path = file_path + table_name + '_' + current_datetime + '.csv'

# Create the directory if it doesn't exist
if not os.path.exists(file_path):
    os.makedirs(file_path)

# Connect to the MySQL database
conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database,
    port=port
)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Select all data from the specified table
query = f"SELECT * FROM {table_name}"

# Execute the query
cursor.execute(query)

# Fetch all the rows from the result set
rows = cursor.fetchall()

# Get the column names
column_names = [desc[0] for desc in cursor.description]

# Write the data to a CSV file
with open(saved_csvfile_path, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)

    # Write the column names as the header
    csv_writer.writerow(column_names)

    # Write the data rows
    csv_writer.writerows(rows)

# Close the cursor and database connection
cursor.close()
conn.close()

print(f"The table '{table_name}' has been exported to '{saved_csvfile_path}'.")
