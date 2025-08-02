
import psycopg2

try:
    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="pvdzxc2003",
        port=5432
    )
    cursor = connection.cursor()

    # Execute a simple query
    cursor.execute("SELECT version();")
    
    db_version = cursor.fetchone()
    print("Connected to PostgreSQL database. Version:", db_version)
    cursor.execute("SET search_path TO public;")
    cursor.execute("SELECT * FROM customer;")
    row = cursor.fetchone()
    while row is not None:
        print(row)

    cursor.close()
    connection.close()

except Exception as error:
    print("Error while connecting to PostgreSQL:", error)