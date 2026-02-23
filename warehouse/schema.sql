-- =============================================
-- Financial Data Warehouse Schema
-- =============================================

-- Raw transactions (landing zone)
CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id VARCHAR(36) PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    timestamp TIMESTAMP NOT NULL,
    merchant_id VARCHAR(20),
    location VARCHAR(100),
    status VARCHAR(20),
    is_flagged BOOLEAN DEFAULT FALSE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cleaned and transformed transactions (processed zone)
CREATE TABLE IF NOT EXISTS processed_transactions (
    transaction_id VARCHAR(36) PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_hour INTEGER,
    amount_bucket VARCHAR(10),
    is_suspicious BOOLEAN DEFAULT FALSE,
    risk_score DECIMAL(3,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily settlement aggregations
CREATE TABLE IF NOT EXISTS daily_settlements (
    settlement_date DATE NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    total_amount DECIMAL(15,2),
    avg_amount DECIMAL(12,2),
    transaction_count INTEGER,
    flagged_count INTEGER,
    running_total DECIMAL(18,2),
    day_over_day_change DECIMAL(8,2),
    PRIMARY KEY (settlement_date, transaction_type)
);

-- Anomaly detection results
CREATE TABLE IF NOT EXISTS anomaly_alerts (
    alert_id SERIAL PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    alert_description TEXT,
    transaction_count INTEGER,
    total_amount DECIMAL(12,2),
    detection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data quality log
CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id SERIAL PRIMARY KEY,
    batch_id VARCHAR(36),
    total_records INTEGER,
    valid_records INTEGER,
    invalid_records INTEGER,
    error_breakdown JSONB,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for query performance
CREATE INDEX IF NOT EXISTS idx_processed_account ON processed_transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_processed_date ON processed_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_settlements_date ON daily_settlements(settlement_date);
CREATE INDEX IF NOT EXISTS idx_anomaly_account ON anomaly_alerts(account_id);
