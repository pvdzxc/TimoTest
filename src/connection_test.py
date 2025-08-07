
import psycopg2

try:
    connection = psycopg2.connect(
<<<<<<< HEAD
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=3636
=======
        host="localhost",
        database="postgres",
        user="postgres",
        password="123456789",
        port=5432
>>>>>>> a871091e71e674c1dabafd8277490e72dc4aa4ed
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