import psycopg2
import csv
import os

# def insert_data_from_csv(cursor, table_name, csv_file_path, columns):
#     with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
#         reader = csv.DictReader(csvfile)
#         for row in reader:
#             values = [row.get(col) for col in columns]
#             placeholders = ','.join(['%s'] * len(columns))
#             query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
#             cursor.execute(query, values)

def main():
    try:
        connection = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="pvdzxc2003",
            port=5432
        )
        cursor = connection.cursor()
        cursor.execute("SET search_path TO public;")

        #debug
        print(os.path.dirname(os.path.abspath(__file__))[:-3])
        base_dir = os.path.dirname(os.path.abspath(__file__))[:-3] + 'data'
        print(os.path.join(base_dir, 'cleaned__sample_customer.csv'))
        # Insert customers
        customer_csv = os.path.join(base_dir, 'cleaned__sample_customer.csv')
        customer_columns = [
            'customer_id', 'first_name', 'last_name', 'phone_number',
            'username', 'email_address', 'national_id', 'dob', 'address'
        ]
        # Map CSV columns to DB columns
        customer_db_columns = [
            'customer_id', 'first_name', 'last_name', 'phone_number',
            'username', 'email_address', 'id_number', 'dob', 'address'
        ]
        # Read and map national_id to id_number for DB
        with open(customer_csv, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                values = [
                    row['customer_id'],
                    row['first_name'],
                    row['last_name'],
                    row['phone_number'],
                    row['username'],
                    row['email_address'],
                    row['national_id'],
                    row['dob'],
                    row['address']
                ]
                values = ['\''+value+'\'' if value else '' for value in values]
                placeholders = ','.join(values)
                # print(f"{', '.join(customer_db_columns)}) VALUES ({placeholders}")
                query = f"INSERT INTO Customer ({', '.join(customer_db_columns)}) VALUES ({placeholders})"
                try:
                    cursor.execute(query, values)
                except psycopg2.IntegrityError as e:
                    print(f"Error inserting customer {row['customer_id']}: {e}")
                    # connection.rollback()
            print(f"Customers inserted successfully. Total: {reader.line_num} rows.")
        # Insert accounts
        account_csv = os.path.join(base_dir, 'cleaned__sample_account.csv')
        account_columns = [
            'account_id', 'customer_id', 'account_number', 'account_balance',
            'account_open_date', 'account_status', 'account_type'
        ]
        account_db_columns = [
            'account_id', 'customer_id', 'account_number', 'account_balance',
            'account_open_date', 'account_status', 'account_type'
        ]
        with open(account_csv, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                values = [
                    row['account_id'],
                    row['customer_id'],
                    row['account_number'],
                    row['account_balance'],
                    row['account_open_date'],
                    row['account_status'],
                    row['account_type']
                ]
                values = ['\''+value+'\'' if value else '' for value in values]
                placeholders = ','.join(values)
                query = f"INSERT INTO BankAccount ({', '.join(account_db_columns)}) VALUES ({placeholders})"
                try:
                    cursor.execute(query, values)
                except psycopg2.IntegrityError as e:
                    print(f"Error inserting BankAccount {row['account_id']}: {e}")
                    connection.rollback()
            print(f"Accounts inserted successfully. Total: {reader.line_num} rows.")
            
        # Insert devices
        device_csv = os.path.join(base_dir, 'cleaned__sample_device.csv')
        device_columns = [
            'device_id', 'customer_id', 'device_name', 'device_model',
            'ip_address', 'is_verified'
        ]
        device_db_columns = [
            'device_id', 'customer_id', 'device_name', 'device_model',
            'ip_address', 'is_verified'
        ]
        with open(device_csv, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                values = [
                    row['device_id'],
                    row['customer_id'],
                    row['device_name'],
                    row['device_model'],
                    row['ip_address'],
                    row['is_verified']
                ]
                values = ['\''+value+'\'' if value else '' for value in values]
                placeholders = ','.join(values)
                query = f"INSERT INTO Device ({', '.join(device_db_columns)}) VALUES ({placeholders})"
                try:
                    cursor.execute(query, values)
                except psycopg2.IntegrityError as e:
                    print(f"Error inserting Device {row['device_id']}: {e}")
            print(f"Devices inserted successfully. Total: {reader.line_num} rows.")
        # Insert auth logs
        auth_csv = os.path.join(base_dir, 'cleaned__sample_auth_log.csv')
        auth_columns = [
            'auth_id', 'device_id', 'customer_id', 'auth_method', 'auth_time'
        ]
        auth_db_columns = [
            'auth_id', 'device_id', 'customer_id', 'auth_method', 'auth_time'
        ]
        with open(auth_csv, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                values = [
                    row['auth_id'],
                    row['device_id'],
                    row['customer_id'],
                    row['auth_method'],
                    row['auth_time']
                ]
                values = ['\''+value+'\'' if value else '' for value in values]
                placeholders = ','.join(values)
                query = f"INSERT INTO AuthenticationLog ({', '.join(auth_db_columns)}) VALUES ({placeholders})"
                try:
                    cursor.execute(query, values)
                except psycopg2.IntegrityError as e:
                    print(f"Error inserting AuthenticationLog {row['auth_id']}: {e}")
            print(f"Auth logs inserted successfully. Total: {reader.line_num} rows.")

        # Insert transactions
        transaction_csv = os.path.join(base_dir, 'cleaned__sample_transaction.csv')
        transaction_columns = [
            'transaction_id', 'account_id', 'amount', 'transaction_time', 'auth_id'
        ]
        transaction_db_columns = [
            'transaction_id', 'account_id', 'amount', 'transaction_time', 'auth_id'
        ]
        with open(transaction_csv, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                values = [
                    row['transaction_id'],
                    row['account_id'],
                    row['amount'],
                    row['transaction_time'],
                    row['auth_id']
                ]
                values = ['\''+value+'\'' if value else '' for value in values]
                placeholders = ','.join(values)
                query = f"INSERT INTO TabTransaction ({', '.join(transaction_db_columns)}) VALUES ({placeholders})"
                try:
                    cursor.execute(query, values)
                except psycopg2.IntegrityError as e:
                    print(f"Error inserting TabTransaction {row['transaction_id']}: {e}")
        connection.commit()
        print("Data inserted successfully.")

        cursor.close()
        connection.close()

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)

if __name__ == "__main__":
    main()