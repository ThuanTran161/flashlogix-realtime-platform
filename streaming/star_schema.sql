-- =============================================================================
-- FlashLogix Kimball Star Schema DDL (Gold Layer)
-- Target: PostgreSQL / ClickHouse / Analytical Store
-- =============================================================================

-- Dim_Date: Date Dimension Table
CREATE TABLE IF NOT EXISTS Dim_Date (
    date_key INT PRIMARY KEY,              -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    day_of_week VARCHAR(15),
    month INT,
    quarter INT,
    year INT,
    is_weekend BOOLEAN
);

-- Dim_Users: Customer Dimension Table (SCD Type 2 Historical Tracking)
CREATE TABLE IF NOT EXISTS Dim_Users (
    user_sk SERIAL PRIMARY KEY,             -- Surrogate Key
    masked_user_id VARCHAR(64) NOT NULL,    -- PII Masked Key (SHA-256)
    membership_tier VARCHAR(20),            -- Standard, VIP, Platinum
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Dim_Products: Product Dimension Table
CREATE TABLE IF NOT EXISTS Dim_Products (
    product_sk SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    base_price DECIMAL(12, 2) NOT NULL
);

-- Fact_Flash_Sale_Orders: Core Flash Sale Transaction Fact Table
CREATE TABLE IF NOT EXISTS Fact_Flash_Sale_Orders (
    order_id INT PRIMARY KEY,
    user_sk INT REFERENCES Dim_Users(user_sk),
    product_sk INT REFERENCES Dim_Products(product_sk),
    date_key INT REFERENCES Dim_Date(date_key),
    quantity INT NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    latency_ms INT,                         -- Latency from placement to completion
    is_fraudulent BOOLEAN DEFAULT FALSE,    -- Flag for automated bot activity
    created_at TIMESTAMP NOT NULL
);
