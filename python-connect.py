import mysql.connector
from mysql.connector import errorcode

# Connect to the MySQL database
connection = mysql.connector.connect(
    user="ilokuda",
    password="ilokudatangjiro",
    host="localhost",
    database="pyspark_database",
    raise_on_warnings=True
)
# except mysql.connector.Error as err:
#   if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#     print("Something is wrong with your user name or password")
#   elif err.errno == errorcode.ER_BAD_DB_ERROR:
#     print("Database does not exist")
#   else:
#     print(err)
# else:
#   connection.close()


# connection.close()
cursor = connection.cursor()

# # Define the SQL query to select all data from the Weather table
sql_query = "SELECT * FROM Weather"

# # Execute the SQL query
cursor.execute(sql_query)

# # Fetch all rows from the result set
rows = cursor.fetchall()

# # Close the cursor and connection
cursor.close()
connection.close()

# # Display the fetched rows
for row in rows:
    print(row)
