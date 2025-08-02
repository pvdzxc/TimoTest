import random
from faker import Faker
from datetime import datetime, timedelta
import csv
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

fake = Faker('en_US')

NUM_CUSTOMERS = 10
NUM_TRANSACTIONS = 100

def create_customers(num):
    customers = []
    for i in range(1, num+1):
        first_name = fake.first_name()
        last_name = fake.last_name()
        username = f"{first_name.lower()}.{last_name.lower()}{random.randint(1,99)}"
        customers.append({
            "customer_id": i,
            "first_name": first_name,
            "last_name": last_name,
            "phone_number": '0' + fake.numerify(text='#########'),  # 10-digit phone number
            "username": username,
            "email_address": f"{username}@{fake.free_email_domain()}",
            "national_id": fake.unique.numerify(text="############"),  # 12-digit ID
            "dob": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
            "address": fake.address()
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

def create_accounts(customers):
    # Generate account data for each customer
    accounts = []
    _i = 0
    for customer in customers:
        for i in range(_i, _i + random.randint(1, 3)):  # Each customer can have 1-3 accounts
            customer_id = customer['customer_id']
            _i += 1
            accounts.append({
                "account_id": i,
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

def create_devices(customers):
    devices = []
    _id = 0
    for customer in customers:
        _id = _id + 1
        _id_device = random.randint(1, len(portable_device_models))
        devices.append({ 
            'device_id': _id + 1,  
            'customer_id': customer['customer_id'],
            'device_name': f"{customer['first_name']} {portable_device_models[_id_device - 1]}",
            'device_model': portable_device_models[_id_device - 1],
            'device_fingerprint': fake.sha256(),
            'device_biometric': fake.sha256(),
            'ip_address': fake.ipv4(),
            'is_verified': random.choice([True, False])
        })
    return devices

def create_transactions(accounts, devices, customers):
    txns = []
    auth = []
    for i in range(NUM_TRANSACTIONS):
        
        acc = random.choice(accounts)
        if acc['account_status'] != 'Active':
            continue
        customer = random.choice([c for c in customers if c['customer_id'] == acc['customer_id']])
        dev = random.choice([d for d in devices if d['customer_id'] == customer['customer_id']])
        time = fake.date_time_between(start_date='-3d', end_date='now')
        dtime = round(random.uniform(0.5, 4),2)
        txns.append({
            'transaction_id': i+1,
            'account_id': acc['account_id'],
            'amount': round(random.uniform(-20000000, 20000000), 2), 
            'transaction_time': time,
            'auth_id': i + 1,
        })
        dtime = dtime * dtime # Simulate authentication logs for each transaction with a random delay
        auth.append({
            'auth_id': i + 1,
            'device_id': dev['device_id'],
            'customer_id': customer['customer_id'],
            'auth_method': random.choice(['OTP', 'Biometric', 'Password', 'PIN']),
            'auth_time': time + timedelta(dtime),
        })
    return txns, auth

if __name__ == "__main__":
    customers = create_customers(NUM_CUSTOMERS)
    accounts = create_accounts(customers)
    # Create devices and auth logs based on customers
    devices = create_devices(customers)
    txns,auth = create_transactions(accounts, devices,customers)

    with open('/home/timobank/TimoTest/data/sample_customer.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)

    with open('/home/timobank/TimoTest/data/sample_transaction.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=txns[0].keys())
        writer.writeheader()
        writer.writerows(txns)
    with open('/home/timobank/TimoTest/data/sample_device.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=devices[0].keys())
        writer.writeheader()
        writer.writerows(devices)
    with open('/home/timobank/TimoTest/data/sample_account.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=accounts[0].keys())
        writer.writeheader()
        writer.writerows(accounts)
    with open('/home/timobank/TimoTest/data/sample_auth.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=auth[0].keys())
        writer.writeheader()
        writer.writerows(auth)

    print("Sample data generated.")