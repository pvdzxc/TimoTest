import random
from faker import Faker
from datetime import datetime, timedelta
import csv
import psycopg2

# Connecting (test) PostgreSQL database
max_customer_id = 0
max_device_id = 0
max_account_id = 0
max_transaction_id = 0
max_auth_id = 0
try:
    connection = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cursor = connection.cursor()

    # Execute a simple query
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    print("Connected to PostgreSQL database. Version:", db_version)
    
    cursor.execute("SET SCHEMA 'public';")
    cursor.execute("SELECT MAX(customer_id) FROM Customer;")
    res = cursor.fetchone()
    max_customer_id = res[0] if res[0] is not None else 0
    cursor.execute("SELECT MAX(transaction_id) FROM TabTransaction;")
    res = cursor.fetchone()
    max_customer_id = res[0] if res[0] is not None else 0
    cursor.execute("SELECT MAX(device_id) FROM Device;")
    res = cursor.fetchone()
    max_device_id = res[0] if res[0] is not None else 0
    cursor.execute("SELECT MAX(account_id) FROM BankAccount;")
    res = cursor.fetchone()
    max_account_id = res[0] if res[0] is not None else 0
    cursor.execute("SELECT MAX(auth_id) FROM AuthenticationLog;")
    res = cursor.fetchone()
    max_auth_id = res[0] if res[0] is not None else 0
    
    cursor.close()
    connection.close()

except Exception as error:
    print("Error while workings to PostgreSQL:", error)
print(f"Max customer ID: {max_customer_id}, Max transaction ID: {max_transaction_id}")
fake = Faker('en_US')

NUM_CUSTOMERS = 100
NUM_TRANSACTIONS = 500

def create_customers(num):
    customers = []
    for i in range(1, num+1):
        first_name = fake.first_name()
        last_name = fake.last_name()
        username = f"{first_name.lower()}.{last_name.lower()}{random.randint(1,99)}"
        customers.append({
            "customer_id": max_customer_id + i,
            "first_name": first_name,
            "last_name": last_name,
            "phone_number": '0' + fake.numerify(text='#########'),  # 10-digit phone number
            "username": username,
            "email_address": f"{username}@{fake.free_email_domain()}",
            "national_id": fake.unique.numerify(text="############"),  # 12-digit ID
            "dob": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
            "address": fake.address().strip().replace('\n', ' '),  # Remove newlines from address
        })
    return customers

# def create_accounts(customers):
#     accounts = []
#     for customer in customers:
#         accounts.append({
#             'account_id': customer['customer_id'],
#             'customer_id': customer['customer_id'],
#             'account_number': fake.unique.bban(),
#             'account_type': random.choice(['Savings', 'Checking']),
#             'balance': round(random.uniform(1000000, 50000000), 2)
#         })
#     return accounts

ACCOUNT_TYPES = ['Savings', 'Checking', 'Business']
ACCOUNT_STATUS = ['Active', 'Inactive', 'Closed']

def create_accounts(customers, max_account_id):
    # Generate account data for each customer
    accounts = []
    for customer in customers:
        for i in range(0, random.randint(1, 3)):  # Each customer can have 1-3 accounts
            customer_id = customer['customer_id']
            max_account_id = max_account_id + 1
            accounts.append({
                "account_id": max_account_id,
                "customer_id": customer_id,
                "account_number": fake.unique.bban(),  # BBAN format (Basic Bank Account Number)
                "account_balance": round(random.uniform(500000, 50000000), 2),
                "account_open_date": fake.date_between(start_date='-5y', end_date='today'),
                "account_status": random.choice(ACCOUNT_STATUS),
                "account_type": random.choice(ACCOUNT_TYPES)
            })
    return accounts

portable_device_models = [
    # IPhone series
    "iPhone 13", "iPhone 13 Pro", "iPhone 13 Pro Max", "iPhone 12", "iPhone 12 Mini",
    "iPhone 14", "iPhone 14 Plus", "iPhone 14 Pro", "iPhone 14 Pro Max",
    "iPhone SE (2022)", "iPhone 11", "iPhone XR"
    "iPhone XS", "iPhone XS Max", "iPhone X", "iPhone 8", "iPhone 8 Plus",
    
    # Samsung Galaxy series
    "Samsung Galaxy S21", "Samsung Galaxy S21 Ultra", "Samsung Galaxy S20 FE", "Samsung Galaxy Note 20", "Samsung Galaxy Note 10+", "Samsung Galaxy A52", "Samsung Galaxy A72", "Samsung Galaxy M31", "Samsung Galaxy Z Fold 3", "Samsung Galaxy Z Flip 3",

    # Xiaomi
    "Xiaomi Mi 11", "Xiaomi Mi 11 Lite", "Xiaomi Mi 10T Pro", "Xiaomi Redmi Note 10", "Xiaomi Redmi Note 10 Pro", "Xiaomi Redmi 9", "Xiaomi Poco X3 Pro", "Xiaomi Poco F3",

    # Oppo
    "Oppo Reno6", "Oppo Reno5", "Oppo A94", "Oppo A74", "Oppo Find X3 Pro", "Oppo A54", 

    # Vivo
    "Vivo V21", "Vivo Y31", "Vivo Y20s", "Vivo X60 Pro", "Vivo V20", 

    # Realme
    "Realme 8 Pro", "Realme 8", "Realme 7 Pro", "Realme Narzo 30A", "Realme C25",

    # Google Pixel
    "Google Pixel 6", "Google Pixel 6 Pro", "Google Pixel 5", "Google Pixel 4a",

    # OnePlus
    "OnePlus 9", "OnePlus 9 Pro", "OnePlus Nord", "OnePlus 8T",

    # Huawei
    "Huawei P40 Pro", "Huawei Mate 40 Pro", "Huawei Nova 8i", "Huawei Y9a"
]

def create_devices(customers, max_device_id):
    devices = []
    for customer in customers:
        max_device_id = max_device_id + 1
        _id_device = random.randint(1, len(portable_device_models))
        devices.append({ 
            'device_id': max_device_id,  
            'customer_id': customer['customer_id'],
            'device_name': f"{customer['first_name']} {portable_device_models[_id_device - 1]}",
            'device_model': portable_device_models[_id_device - 1],
            'device_fingerprint': fake.sha256(),
            'device_biometric': fake.sha256(),
            'ip_address': fake.ipv4(),
            'is_verified': random.choice([True, False])
        })
    return devices

def create_transactions(accounts, devices, customers, max_transaction_id, max_auth_id):
    txns = []
    auth = []
    for i in range(1, NUM_TRANSACTIONS +1):
        max_transaction_id = max_transaction_id + 1
        max_auth_id = max_auth_id + 1
        acc = random.choice(accounts)
        if acc[2] != 'Active':
            continue
        for cus in customers:
            if cus[0] == acc[1]:
                customer = cus
                break
        dev = random.choice([d for d in devices if d[1] == customer[0]])
        time = fake.date_time_between(start_date='-3d', end_date='now')
        dtime = round(random.uniform(0.5, 4),2)
        txns.append({
            'transaction_id': max_transaction_id,
            'account_id': acc[0],
            'amount': round(random.uniform(-20000000, 20000000), 2), 
            'transaction_time': time,
            'auth_id': max_auth_id,
        })
        dtime = dtime * dtime # Simulate authentication logs for each transaction with a random delay
        auth.append({
            'auth_id': max_auth_id,
            'device_id': dev[0],
            'customer_id': customer[0],
            'auth_method': random.choice(['OTP', 'Biometric', 'Password', 'PIN']),
            'auth_time': time + timedelta(dtime),
        })
    return txns, auth

if __name__ == "__main__":
    customers = create_customers(NUM_CUSTOMERS)
    accounts = create_accounts(customers, max_account_id)
    devices = create_devices(customers, max_device_id)
    
    try:
        connection = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow",
            port=5432
        )
        cursor = connection.cursor()
        cursor.execute("SET SCHEMA 'public';")
        cursor.execute("SELECT customer_id, first_name, last_name FROM Customer;")
        existing_customers = cursor.fetchall()
        existing_customers = [[cust[0], cust[1], cust[2]] for cust in existing_customers]
        if existing_customers:
            print(f"Found {len(existing_customers)} existing customers in the database.")
        new_customers = [[cust['customer_id'], cust['first_name'], cust['last_name']] for cust in customers]
        existing_customers = existing_customers + new_customers
        cursor.execute("SELECT account_id, customer_id, account_status FROM BankAccount;")
        existing_accounts = cursor.fetchall()
        existing_accounts = [[acc[0], acc[1], acc[2]] for acc in existing_accounts]
        if existing_accounts:
            print(f"Found {len(existing_accounts)} existing accounts in the database.")
        new_accounts = [[acc['account_id'], acc['customer_id'], acc['account_status']] for acc in accounts]
        existing_accounts = existing_accounts + new_accounts
        
        cursor.execute("SELECT device_id, customer_id FROM Device;")
        existing_devices = cursor.fetchall()
        existing_devices = [[dev[0], dev[1]] for dev in existing_devices]
        if existing_devices:
            print(f"Found {len(existing_devices)} existing devices in the database.")
        new_devices = [[dev['device_id'], dev['customer_id']] for dev in devices]
        existing_devices = existing_devices + new_devices
        
        cursor.close()
        connection.close()
    except Exception as error:
        print("Error while retrieving existing accounts and devices:", error)
    
    txns,auth = create_transactions(existing_accounts, existing_devices, existing_customers, max_transaction_id, max_auth_id)

    with open('/opt/airflow/data/sample_customer.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)

    with open('/opt/airflow/data/sample_transaction.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=txns[0].keys())
        writer.writeheader()
        writer.writerows(txns)
    with open('/opt/airflow/data/sample_device.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=devices[0].keys())
        writer.writeheader()
        writer.writerows(devices)
    with open('/opt/airflow/data/sample_account.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=accounts[0].keys())
        writer.writeheader()
        writer.writerows(accounts)
    with open('/opt/airflow/data/sample_auth.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=auth[0].keys())
        writer.writeheader()
        writer.writerows(auth)

    print("Sample data generated.")