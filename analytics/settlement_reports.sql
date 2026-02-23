-- =============================================
-- Financial Data Pipeline — SQL Analytics
-- Settlement Reports & Anomaly Analysis
-- =============================================
-- These queries demonstrate JOINs, CTEs, and
-- window functions (SUM OVER, LAG, RANK, ROW_NUMBER).
-- =============================================


-- =============================================
-- Query 1: Daily Settlement Summary with Running Totals
-- Uses: SUM OVER (window), LAG (window)
-- =============================================
SELECT
    settlement_date,
    transaction_type,
    transaction_count,
    total_amount,
    avg_amount,
    flagged_count,
    running_total,
    -- Day-over-day absolute change
    total_amount - LAG(total_amount, 1) OVER (
        PARTITION BY transaction_type
        ORDER BY settlement_date
    ) AS dod_change,
    -- Day-over-day percentage change
    ROUND(
        (total_amount - LAG(total_amount, 1) OVER (
            PARTITION BY transaction_type
            ORDER BY settlement_date
        )) / NULLIF(LAG(total_amount, 1) OVER (
            PARTITION BY transaction_type
            ORDER BY settlement_date
        ), 0) * 100,
        2
    ) AS dod_pct_change
FROM daily_settlements
ORDER BY settlement_date DESC, transaction_type;


-- =============================================
-- Query 2: Top 10 Accounts by Transaction Volume
-- Uses: JOIN between processed_transactions and anomaly_alerts
-- =============================================
SELECT
    pt.account_id,
    COUNT(pt.transaction_id) AS total_transactions,
    SUM(pt.amount) AS total_volume,
    ROUND(AVG(pt.amount), 2) AS avg_transaction_amount,
    COUNT(DISTINCT pt.transaction_date) AS active_days,
    SUM(CASE WHEN pt.is_suspicious THEN 1 ELSE 0 END) AS suspicious_count,
    COALESCE(aa.alert_count, 0) AS alert_count,
    COALESCE(aa.alert_types, 'NONE') AS alert_types
FROM processed_transactions pt
LEFT JOIN (
    SELECT
        account_id,
        COUNT(*) AS alert_count,
        STRING_AGG(DISTINCT alert_type, ', ') AS alert_types
    FROM anomaly_alerts
    GROUP BY account_id
) aa ON pt.account_id = aa.account_id
GROUP BY pt.account_id, aa.alert_count, aa.alert_types
ORDER BY total_volume DESC
LIMIT 10;


-- =============================================
-- Query 3: Hourly Transaction Distribution with Anomaly Rate
-- Uses: CTE, aggregation, percentage calculation
-- =============================================
WITH hourly_stats AS (
    SELECT
        transaction_hour,
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_amount,
        ROUND(AVG(amount), 2) AS avg_amount,
        SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END) AS suspicious_count
    FROM processed_transactions
    GROUP BY transaction_hour
),
hourly_with_rates AS (
    SELECT
        transaction_hour,
        total_transactions,
        total_amount,
        avg_amount,
        suspicious_count,
        ROUND(
            suspicious_count::DECIMAL / NULLIF(total_transactions, 0) * 100,
            2
        ) AS anomaly_rate_pct
    FROM hourly_stats
)
SELECT
    transaction_hour,
    total_transactions,
    total_amount,
    avg_amount,
    suspicious_count,
    anomaly_rate_pct,
    CASE
        WHEN anomaly_rate_pct > 20 THEN 'HIGH RISK'
        WHEN anomaly_rate_pct > 10 THEN 'MEDIUM RISK'
        ELSE 'LOW RISK'
    END AS risk_level
FROM hourly_with_rates
ORDER BY transaction_hour;


-- =============================================
-- Query 4: Merchant Risk Analysis
-- Uses: RANK, DENSE_RANK (window), JOIN with daily_settlements
-- =============================================
WITH merchant_stats AS (
    SELECT
        pt.merchant_id,
        COUNT(*) AS total_transactions,
        SUM(pt.amount) AS total_volume,
        SUM(CASE WHEN pt.is_suspicious THEN 1 ELSE 0 END) AS flagged_transactions,
        ROUND(
            SUM(CASE WHEN pt.is_suspicious THEN 1 ELSE 0 END)::DECIMAL
            / NULLIF(COUNT(*), 0) * 100,
            2
        ) AS flagged_pct
    FROM processed_transactions pt
    GROUP BY pt.merchant_id
    HAVING COUNT(*) >= 5
)
SELECT
    ms.merchant_id,
    ms.total_transactions,
    ms.total_volume,
    ms.flagged_transactions,
    ms.flagged_pct,
    RANK() OVER (ORDER BY ms.flagged_pct DESC) AS risk_rank,
    DENSE_RANK() OVER (ORDER BY ms.flagged_pct DESC) AS risk_dense_rank
FROM merchant_stats ms
ORDER BY risk_rank
LIMIT 20;


-- =============================================
-- Query 5: Account Behavior Pattern Detection
-- Uses: CTE, LAG, LEAD, ROW_NUMBER (window)
-- Detects rapid successive transactions
-- =============================================
WITH ordered_transactions AS (
    SELECT
        transaction_id,
        account_id,
        amount,
        transaction_type,
        transaction_date,
        transaction_hour,
        risk_score,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY transaction_date, transaction_hour
        ) AS txn_sequence_num,
        LAG(amount, 1) OVER (
            PARTITION BY account_id
            ORDER BY transaction_date, transaction_hour
        ) AS prev_amount,
        LEAD(amount, 1) OVER (
            PARTITION BY account_id
            ORDER BY transaction_date, transaction_hour
        ) AS next_amount,
        LAG(transaction_type, 1) OVER (
            PARTITION BY account_id
            ORDER BY transaction_date, transaction_hour
        ) AS prev_type
    FROM processed_transactions
),
pattern_flags AS (
    SELECT
        *,
        -- Flag: same type in succession
        CASE WHEN transaction_type = prev_type THEN TRUE ELSE FALSE END AS repeated_type,
        -- Flag: increasing amounts in succession
        CASE WHEN amount > COALESCE(prev_amount, 0)
              AND COALESCE(next_amount, 0) > amount
             THEN TRUE ELSE FALSE END AS escalating_pattern,
        -- Flag: large deviation from previous transaction
        CASE WHEN prev_amount IS NOT NULL
              AND ABS(amount - prev_amount) / NULLIF(prev_amount, 0) > 5
             THEN TRUE ELSE FALSE END AS large_deviation
    FROM ordered_transactions
)
SELECT
    account_id,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN repeated_type THEN 1 ELSE 0 END) AS repeated_type_count,
    SUM(CASE WHEN escalating_pattern THEN 1 ELSE 0 END) AS escalating_count,
    SUM(CASE WHEN large_deviation THEN 1 ELSE 0 END) AS deviation_count,
    ROUND(AVG(risk_score), 3) AS avg_risk_score,
    ROUND(MAX(risk_score), 3) AS max_risk_score
FROM pattern_flags
GROUP BY account_id
HAVING SUM(CASE WHEN repeated_type THEN 1 ELSE 0 END) > 2
    OR SUM(CASE WHEN escalating_pattern THEN 1 ELSE 0 END) > 1
    OR SUM(CASE WHEN large_deviation THEN 1 ELSE 0 END) > 0
ORDER BY avg_risk_score DESC
LIMIT 20;
