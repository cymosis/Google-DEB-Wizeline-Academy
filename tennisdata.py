import os
import csv
import sqlite3
import zipfile
from datetime import datetime
import mysql.connector


# Set the working directory to the location where you want to extract the files
working_directory = "C:/Users/Hannah_IT/Desktop/tennis_atp-master"



# Define a function to get the list of files to ingest
def get_files_list(files, pattern):
    files_known = [file for file in files if file.startswith(pattern)]
    return files_known

# Get the list of files to process
files_to_process = os.listdir()
files_to_ingest = get_files_list(files_to_process, 'atp_matches_')

# Initialize a list to store ATP matches data
atp_matches = []

# Process and ingest ATP matches data
for file in files_to_ingest:
    with open(file, 'r') as atp_file:
        csv_reader = csv.reader(atp_file)
        atp_matches.extend(list(csv_reader))

# Select specific columns of interest
columns_of_interest = [
    'tourney_id', 'tourney_name', 'surface', 'draw_size', 'tourney_level',
    'tourney_date', 'match_num', 'filename', 'time_stamp'
]

# Create a new list with selected columns
selected_atp_matches = [row[:9] + [file, datetime.now().strftime('%Y-%m-%d %H:%M:%S')] for row in atp_matches]
# selected_atp_matches_tuples = [tuple(row) for row in selected_atp_matches]


# Write the selected data to a CSV file
output_csv_file = 'all_matches.csv'
with open(output_csv_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(selected_atp_matches)


# Replace with your actual database configuration
db_config = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',  # This could be 'localhost' or an IP address
    'database': 'tennis_atp',
}

try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS atp_matches (
        tourney_id VARCHAR(255), tourney_name VARCHAR(255), surface VARCHAR(255),
        draw_size INT, tourney_level VARCHAR(255), tourney_date DATE,
        match_num INT, filename VARCHAR(255), time_stamp DATETIME
    )
    """
    cursor.execute(create_table_query)
    conn.commit()


    # Load data into the SQLite table
    with open(output_csv_file, 'r') as f:
        csv_reader = csv.reader(f)
        data_to_insert = [tuple(row) for row in csv_reader if row]  # Filter out empty rows

    # Insert the data into the MariaDB table
    insert_query = """
    INSERT INTO atp_matches
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data_to_insert)
    conn.commit()

    print("CSV data inserted into the MariaDB table successfully!")



except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("Connection closed.")
