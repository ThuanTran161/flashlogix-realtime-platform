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

CREATE INDEX IF NOT EXISTS idx_dim_users_masked_current
    ON Dim_Users (masked_user_id, is_current);

-- Dim_Products: Product Dimension Table
CREATE TABLE IF NOT EXISTS Dim_Products (
    product_sk SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    base_price DECIMAL(12, 2) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_products_product_id
    ON Dim_Products (product_id);

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

CREATE INDEX IF NOT EXISTS idx_fact_orders_date_product
    ON Fact_Flash_Sale_Orders (date_key, product_sk);

CREATE INDEX IF NOT EXISTS idx_fact_orders_user_sk
    ON Fact_Flash_Sale_Orders (user_sk);

CREATE INDEX IF NOT EXISTS idx_fact_orders_fraud
    ON Fact_Flash_Sale_Orders (is_fraudulent);
