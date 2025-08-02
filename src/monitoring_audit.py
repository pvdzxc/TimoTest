import psycopg2
import logging

# Set up logging
logging.basicConfig(
    filename='/home/timobank/TimoTest/logs/risk_audit_log.txt',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

STRONG_AUTH = ("Biometric", "OTP")

def main():
    try:
        connection = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="123456789",
            port=5432
        )
        cursor = connection.cursor()

        # 1. Transactions > 10M VND must use strong auth
        cursor.execute("""
            SELECT t.transaction_id, t.amount, a.auth_method, c.customer_id
            FROM TabTransaction t
            JOIN AuthenticationLog a ON t.auth_id = a.auth_id
            JOIN BankAccount b ON t.account_id = b.account_id
            JOIN Customer c ON b.customer_id = c.customer_id
            WHERE t.amount > 10000000 AND a.auth_method NOT IN %s
        """, (STRONG_AUTH,))
        rows = cursor.fetchall()
        for row in rows:
            # Set the risk_tag field to 'weak_authentication'
            cursor.execute(
                "UPDATE TabTransaction SET risk_tag = %s WHERE transaction_id = %s",
                ('weak_authentication', row[0])
            )
            logging.warning(f"Risk detected on transaction {row[0]}: Amount {row[1]} VND used weak auth ({row[2]}) for customer {row[3]}.")

        # 2. Device must be verified if new or untrusted
        cursor.execute("""
            SELECT d.device_id, d.customer_id, d.is_verified
            FROM Device d
            LEFT JOIN (
                SELECT device_id as auth_device, MIN(transaction_time) as first_use
                FROM TabTransaction t
                JOIN AuthenticationLog a ON t.auth_id = a.auth_id
                GROUP BY device_id
            ) first_txn ON d.device_id = first_txn.auth_device
            WHERE d.is_verified = FALSE
        """)
        rows = cursor.fetchall()
        for row in rows:
            logging.warning(f"Device {row[0]} for customer {row[1]} is unverified.")

        # 3. Total transaction amount per customer > 20M VND in a day must have at least one strong auth
        cursor.execute("""
            SELECT c.customer_id, DATE(t.transaction_time) as txn_date, SUM(t.amount) as total_amount
            FROM TabTransaction t
            JOIN BankAccount b ON t.account_id = b.account_id
            JOIN Customer c ON b.customer_id = c.customer_id
            GROUP BY c.customer_id, DATE(t.transaction_time)
            HAVING SUM(t.amount) > 20000000
        """)
        high_total_rows = cursor.fetchall()
        for customer_id, txn_date, total_amount in high_total_rows:
            cursor.execute("""
                SELECT COUNT(*) FROM TabTransaction t
                JOIN AuthenticationLog a ON t.auth_id = a.auth_id
                JOIN BankAccount b ON t.account_id = b.account_id
                WHERE b.customer_id = %s
                  AND DATE(t.transaction_time) = %s
                  AND a.auth_method IN %s
            """, (customer_id, txn_date, STRONG_AUTH))
            strong_auth_count = cursor.fetchone()[0]
            if strong_auth_count == 0:
                logging.warning(
                    f"Risk detected customer {customer_id} had total transactions {total_amount} VND on {txn_date} without any strong authentication method."
                )
            # Set the risk_tag field to 'weak_authentication'
            cursor.execute(
                f"UPDATE Customer SET risk_tag = 'weak_authentication' WHERE customer_id = '{customer_id}'"
            )
        cursor.close()
        connection.close()
    except Exception as error:
        logging.error(f"Error during risk audit: {error}")

if __name__ == "__main__":
    main()