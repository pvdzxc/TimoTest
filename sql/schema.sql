SET SCHEMA 'public';
DROP TABLE IF EXISTS Customer CASCADE;
DROP TABLE IF EXISTS BankAccount CASCADE;
DROP TABLE IF EXISTS Device CASCADE;
DROP TABLE IF EXISTS AuthenticationLog CASCADE;
DROP TABLE IF EXISTS RiskTag CASCADE;
DROP TABLE IF EXISTS TabTransaction CASCADE;   

CREATE TABLE Customer (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone_number VARCHAR(20),
    date_joined TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    username VARCHAR(100) UNIQUE NOT NULL,
    email_address VARCHAR(255) UNIQUE NOT NULL,
    id_number VARCHAR(20) UNIQUE NOT NULL,
    dob DATE,
    address TEXT,
    risk_tag VARCHAR(100)  -- e.g., strong_authentication, weak_authentication
);

CREATE TABLE BankAccount (
    account_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES Customer(customer_id),
    account_number VARCHAR(20) UNIQUE NOT NULL,
    account_balance DECIMAL(18,2),
    currency VARCHAR(10) DEFAULT 'VND',  -- Default currency can be changed as needed
    account_open_date DATE NOT NULL,
    account_status VARCHAR(20),  -- e.g., Active, Inactive, Closed
    account_type VARCHAR(50)     -- e.g., Savings, Checking
);

CREATE TABLE Device (
    device_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES Customer(customer_id),
    device_name TEXT,
    device_model VARCHAR(100),
    ip_address VARCHAR(45),
    is_verified BOOLEAN DEFAULT FALSE,
    device_status VARCHAR(20)  -- e.g., Active, Inactive, Blocked
);

CREATE TABLE AuthenticationLog (
    auth_id SERIAL PRIMARY KEY,
    device_id INT REFERENCES Device(device_id),
    auth_method VARCHAR(50),  -- e.g., OTP, Biometric
    auth_status VARCHAR(20),  -- e.g., Success, Failed
    customer_id INT REFERENCES Customer(customer_id),
    auth_time TIMESTAMP
);

CREATE TABLE TabTransaction (
    transaction_id SERIAL PRIMARY KEY,
    account_id INT REFERENCES BankAccount(account_id),
    amount DECIMAL(18,2),
    transaction_time TIMESTAMP,
    auth_id INT REFERENCES AuthenticationLog(auth_id),
    risk_tag VARCHAR(100)  -- e.g. strong_authentication, weak_authentication
);
