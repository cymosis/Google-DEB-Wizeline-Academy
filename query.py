#import psycopg2
import mysql.connector

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


except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("Connection closed.")