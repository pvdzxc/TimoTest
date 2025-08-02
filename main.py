import psycopg2

# Connect to your PostgreSQL database
try:
    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="123456789",
        port=5432
    )
    cursor = connection.cursor()

    # Execute a simple query
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    print("Connected to PostgreSQL database. Version:", db_version)

    cursor.close()
    connection.close()

except Exception as error:
    print("Error while connecting to PostgreSQL:", error)
