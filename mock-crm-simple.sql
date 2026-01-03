-- Simple Mock CRM Database Setup for Airbyte Testing

CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    company VARCHAR(255),
    customer_type VARCHAR(50) NOT NULL,
    registration_date DATE NOT NULL,
    credit_limit DECIMAL(10,2) DEFAULT 0.00,
    active BOOLEAN DEFAULT true,
    city VARCHAR(100),
    state VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS auction_registrations (
    registration_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    auction_date DATE NOT NULL,
    auction_location VARCHAR(100),
    registration_status VARCHAR(50) NOT NULL,
    bidder_number VARCHAR(20),
    deposit_amount DECIMAL(10,2),
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (first_name, last_name, email, company, customer_type, registration_date, credit_limit, active, city, state)
VALUES 
    ('John', 'Smith', 'john.smith@smithconstruction.com', 'Smith Construction LLC', 'buyer', '2024-01-15', 50000.00, true, 'Wichita', 'KS'),
    ('Emily', 'Brown', 'emily.brown@browntrucking.com', 'Brown Trucking Inc', 'buyer', '2024-03-25', 100000.00, true, 'Wichita', 'KS'),
    ('Mike', 'Williams', 'mwilliams@wequipment.com', 'Williams Equipment Rental', 'seller', '2024-02-20', 0.00, true, 'Wichita', 'KS'),
    ('Sarah', 'Johnson', 'sarah@johnsonenterprises.com', 'Johnson Enterprises', 'both', '2024-01-05', 200000.00, true, 'Wichita', 'KS'),
    ('David', 'Martinez', 'dmartinez@martinezgroup.com', 'Martinez Equipment Group', 'both', '2024-03-10', 175000.00, true, 'Kansas City', 'KS')
ON CONFLICT (email) DO NOTHING;

INSERT INTO auction_registrations (customer_id, auction_date, auction_location, registration_status, bidder_number, deposit_amount)
VALUES
    (1, '2024-12-15', 'Wichita, KS', 'approved', 'B1001', 1000.00),
    (2, '2024-12-15', 'Wichita, KS', 'approved', 'B1002', 2000.00),
    (4, '2024-12-15', 'Wichita, KS', 'approved', 'B1005', 3000.00),
    (1, '2024-12-20', 'Kansas City, KS', 'approved', 'B2001', 1000.00),
    (5, '2024-12-20', 'Kansas City, KS', 'approved', 'B2003', 3000.00);

SELECT 'Customers loaded:' as info, COUNT(*) as count FROM customers
UNION ALL
SELECT 'Registrations loaded:', COUNT(*) FROM auction_registrations;
