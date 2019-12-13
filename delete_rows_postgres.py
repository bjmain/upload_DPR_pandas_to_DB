#!/usr/bin/python3
# import the module
import psycopg2
import config   #you need to create this config.py file and update the variables with your database, username and password


# Get a database connection
conn_string = "host="+config.HOST+" dbname="+config.DB+" user="+config.username+" password="+config.password

# Get a database connection
conn = psycopg2.connect(conn_string)

# Create a cursor object. Allows us to execute the SQL query
cursor = conn.cursor()

# execute postgres statement
sql_command = "DELETE FROM {}.{};".format(str(config.dpr_schema), str(config.use_data))
cursor.execute (sql_command)

# get the number of updated rows
rows_deleted = cursor.rowcount
print("deleted rows:",rows_deleted)

# Commit the changes to the database
conn.commit()
# Close communication with the PostgreSQL database
cursor.close()
