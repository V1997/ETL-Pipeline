-- Migration script: V1__initial_schema.sql

-- Create tables if they don't exist
CREATE TABLE IF NOT EXISTS sales_batches (
    id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL UNIQUE,
    source VARCHAR(100) NOT NULL,
    record_count INT NOT NULL DEFAULT 0,
    start_time DATETIME NOT NULL,
    end_time DATETIME NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    error_count INT NOT NULL DEFAULT 0,
    created_by VARCHAR(100) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_batch_status (status),
    INDEX idx_batch_created_at (created_at)
);

CREATE TABLE IF NOT EXISTS sales_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    region VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    item_type VARCHAR(100) NOT NULL,
    sales_channel VARCHAR(50) NOT NULL,
    order_priority CHAR(1) NOT NULL,
    order_date DATE NOT NULL,
    ship_date DATE NOT NULL,
    units_sold INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    unit_cost DECIMAL(10, 2) NOT NULL,
    total_revenue DECIMAL(12, 2) NOT NULL,
    total_cost DECIMAL(12, 2) NOT NULL,
    total_profit DECIMAL(12, 2) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    batch_id VARCHAR(50) NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES sales_batches(batch_id),
    INDEX idx_order_date (order_date),
    INDEX idx_region_country (region, country),
    INDEX idx_item_type (item_type),
    INDEX idx_batch_id (batch_id)
);

CREATE TABLE IF NOT EXISTS etl_errors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    error_type VARCHAR(100) NOT NULL,
    error_message TEXT NOT NULL,
    component VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    record_id VARCHAR(100) NULL,
    batch_id VARCHAR(50) NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolution_note TEXT NULL,
    FOREIGN KEY (batch_id) REFERENCES sales_batches(batch_id),
    INDEX idx_error_severity (severity),
    INDEX idx_error_resolved (resolved),
    INDEX idx_error_timestamp (timestamp),
    INDEX idx_error_batch_id (batch_id)
);

CREATE TABLE IF NOT EXISTS etl_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value FLOAT NOT NULL,
    component VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(50) NULL,
    FOREIGN KEY (batch_id) REFERENCES sales_batches(batch_id),
    INDEX idx_metric_name (metric_name),
    INDEX idx_metric_timestamp (timestamp),
    INDEX idx_metric_batch_id (batch_id)
);

CREATE TABLE IF NOT EXISTS etl_audit (
    id INT AUTO_INCREMENT PRIMARY KEY,
    action VARCHAR(100) NOT NULL,
    component VARCHAR(100) NOT NULL,
    details TEXT NULL,
    user_id VARCHAR(100) NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(50) NULL,
    FOREIGN KEY (batch_id) REFERENCES sales_batches(batch_id),
    INDEX idx_audit_action (action),
    INDEX idx_audit_timestamp (timestamp),
    INDEX idx_audit_batch_id (batch_id)
);