import psycopg2
import pandas as pd
import re

# Set up logging
import logging
logging.basicConfig(
    filename='/opt/airflow/logs/data_quality_log.txt',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)
basedir = '/opt/airflow/data'
# Load data sources
customers = pd.read_csv(basedir + '/sample_customer.csv')
transactions = pd.read_csv(basedir + '/sample_transaction.csv')
accounts = pd.read_csv(basedir + '/sample_account.csv')
devices = pd.read_csv(basedir + '/sample_device.csv')
auth_logs = pd.read_csv(basedir + '/sample_auth.csv')

def check_source_missing(table, table_name, check_columns, cascading_tables):
    logging.info(f"[Missing Values Check] {table_name}")
    for column in check_columns:
        missing = table[table[column].isnull()]
        if not missing.empty:
            logging.warning(f"Rows with missing {column} in {table_name}:")
            logging.warning(f"\n{missing}")
            for cascading_table, foreign_key in cascading_tables:
                logging.warning(f"Checking cascading table {cascading_table} for missing {foreign_key} related to {table_name}.{column}")
                related_missing = cascading_table[cascading_table[foreign_key].isin(missing[column])]
                if not related_missing.empty:
                    logging.warning(f"Rows in {cascading_table} with missing {foreign_key} related to {table_name}.{column}:")
                    logging.warning(f"\n{related_missing}")
                cascading_table.dropna(subset=[foreign_key], inplace=True)
            table = table.dropna(subset=[column])
        logging.info(f"Cleaned {table_name} by removing rows with missing {column}.")
    return table

def check_source_unique(table, check_columns, primary_key='customer_id'):
    logging.info(f"[Uniqueness Check]  {check_columns}")
    for column in check_columns:
        if isinstance(column, list):
            duplicates = table.duplicated(subset=column).sum()
            col_names = ', '.join(column)
            if duplicates > 0:
                logging.warning(f"Found {duplicates} duplicate ({col_names}) pairs")
                logging.warning(f"\n{table[table.duplicated(subset=column, keep=False)][[primary_key] + column]}")
            else:
                logging.info(f"All ({col_names}) pairs are unique.")
            table.drop_duplicates(subset=column, keep='first', inplace=True)
        else:
            duplicates = table.duplicated(subset=column).sum()
            if duplicates > 0:
                logging.warning(f"Found {duplicates} duplicate {column}(s)")
                logging.warning(f"\n{table[table.duplicated(subset=column, keep=False)][[primary_key, column]]}")
            else:
                logging.info(f"All {column} values are unique.")
            table.drop_duplicates(subset=column, keep='first', inplace=True)
    return table

def check_source_national_id_format(table):
    logging.info("[Format Check] National ID")
    invalid = table[~table['national_id'].astype(str).str.match(r'^\d{12}$')]
    if not invalid.empty:
        logging.warning(f"Found {len(invalid)} invalid national_id(s):")
        logging.warning(f"\n{invalid[['customer_id', 'national_id']]}")
    else:
        logging.info("All national_id values are properly formatted.")

def check_nulls(cursor, table, columns):
    logging.info(f"[Null Check] {table}")
    for col in columns:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL;")
        count = cursor.fetchone()[0]
        if count > 0:
            logging.warning(f"{count} rows have NULL in '{col}'")
        else:
            logging.info(f"No NULLs in '{col}'")

def check_uniqueness(cursor, table, columns):
    logging.info(f"[Uniqueness Check] {table}")
    for col in columns:
        if isinstance(col, list) and len(col) > 1:
            col_str = ', '.join(col)
            cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT ({col_str})) FROM {table};")
        else:
            col_name = col[0] if isinstance(col, list) else col
            cursor.execute(f"SELECT COUNT({col_name}), COUNT(DISTINCT {col_name}) FROM {table};")
        total, unique = cursor.fetchone()
        if total != unique:
            logging.warning(f"{col} has duplicates ({total - unique} duplicate(s))")
            logging.warning("Non-unique (duplicating) rows:")
            if isinstance(col, list) and len(col) > 1:
                col_str = ', '.join(col)
                cursor.execute(f"SELECT {col_str}, COUNT(*) FROM {table} GROUP BY {col_str} HAVING COUNT(*) > 1;")
            else:
                col_name = col[0] if isinstance(col, list) else col
                cursor.execute(f"SELECT {col_name}, COUNT(*) FROM {table} GROUP BY {col_name} HAVING COUNT(*) > 1;")
            duplicates = cursor.fetchall()
            for dup in duplicates:
                logging.warning(str(dup))
        else:
            logging.info(f"All values in '{col}' are unique")

def check_foreign_keys(cursor, child_table, child_col, parent_table, parent_col):
    logging.info(f"[Foreign Key Check] {child_table}.{child_col} -> {parent_table}.{parent_col}")
    query = f"""
        SELECT COUNT(*) FROM {child_table} c
        LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
        WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL;
    """
    cursor.execute(query)
    count = cursor.fetchone()[0]
    if count > 0:
        logging.warning(f"{count} orphaned foreign keys in {child_table}.{child_col}")
        logging.warning("Orphaned rows:")
        cursor.execute(f"SELECT * FROM {child_table} c WHERE c.{child_col} IS NOT NULL AND c.{child_col} NOT IN (SELECT {parent_col} FROM {parent_table});")
        orphaned_rows = cursor.fetchall()
        for row in orphaned_rows:
            logging.warning(str(row))
    else:
        logging.info(f"All foreign keys in {child_table}.{child_col} are valid")

def main():
    # Run checks
    clean_customers = check_source_missing(customers, "Customers", ['national_id', 'first_name', 'last_name'], [[devices, 'customer_id'], [accounts, 'customer_id'], [auth_logs, 'customer_id']])
    clean_transactions = check_source_missing(transactions, "Transactions", ['amount', 'account_id', 'auth_id'], [[auth_logs, 'auth_id']])
    clean_devices = check_source_missing(devices, "Devices", ['customer_id', 'device_name', 'device_model'], [[auth_logs, 'device_id']])
    clean_accounts = check_source_missing(accounts, "Accounts", ['customer_id', 'account_number', 'account_open_date'],[[transactions, 'account_id']])
    clean_auth_logs = check_source_missing(auth_logs, "Auth Logs", ['device_id', 'customer_id', 'auth_method', 'auth_time'], [[transactions, 'auth_id']])
    # Check for uniqueness
    clean_customers = check_source_unique(clean_customers, ['national_id', 'phone_number', 'email_address'])
    clean_transactions = check_source_unique(clean_transactions, ['auth_id'], primary_key='transaction_id')
    with open(basedir + '/cleaned__sample_customer.csv', 'w') as f:
        clean_customers.to_csv(f, index=False)
    with open(basedir + '/cleaned__sample_transaction.csv', 'w') as f:
        clean_transactions.to_csv(f, index=False)
    with open(basedir + '/cleaned__sample_device.csv', 'w') as f:
        clean_devices.to_csv(f, index=False)
    with open(basedir + '/cleaned__sample_account.csv', 'w') as f:
        clean_accounts.to_csv(f, index=False)
    with open(basedir + '/cleaned__sample_auth_log.csv', 'w') as f:
        clean_auth_logs.to_csv(f, index=False)
    check_source_national_id_format(customers)
    try:
        connection = psycopg2.connect(
            host="localhost",
            database="airflow",
            user="airflow",
            password="airflow",
            port=3636
        )
        cursor = connection.cursor()

        # Null checks
        check_nulls(cursor, "Customer", ["first_name", "last_name", "username", "email_address", "id_number"])
        check_nulls(cursor, "BankAccount", ["customer_id", "account_number", "account_open_date"])
        check_nulls(cursor, "Device", ["customer_id"])
        check_nulls(cursor, "AuthenticationLog", ["device_id", "customer_id", "auth_method", "auth_time"])
        check_nulls(cursor, "TabTransaction", ["account_id", "amount", "transaction_time", "auth_id"])

        # Uniqueness checks
        check_uniqueness(cursor, "Customer", [["username"], ["email_address"], ["id_number"]])
        check_uniqueness(cursor, "BankAccount", [["account_id", "customer_id"], ["account_number"]])
        check_uniqueness(cursor, "Device", [["device_id", "customer_id"]])
        check_uniqueness(cursor, "AuthenticationLog", [["auth_id", "device_id", "customer_id"]])
        check_uniqueness(cursor, "TabTransaction", [["transaction_id", "account_id", "auth_id"]])

        # Foreign key checks
        check_foreign_keys(cursor, "BankAccount", "customer_id", "Customer", "customer_id")
        check_foreign_keys(cursor, "Device", "customer_id", "Customer", "customer_id")
        check_foreign_keys(cursor, "AuthenticationLog", "device_id", "Device", "device_id")
        check_foreign_keys(cursor, "AuthenticationLog", "customer_id", "Customer", "customer_id")
        check_foreign_keys(cursor, "TabTransaction", "account_id", "BankAccount", "account_id")
        check_foreign_keys(cursor, "TabTransaction", "auth_id", "AuthenticationLog", "auth_id")

        cursor.close()
        connection.close()
    except Exception as error:
        logging.error(f"Error while checking data integrity: {error}")

if __name__ == "__main__":
    main()