"""
PostgreSQL Warehouse Loader

Bulk-loads DataFrames and record lists into the PostgreSQL data warehouse.
Handles duplicates with ON CONFLICT, supports batch inserts for performance.
"""

import json
import uuid
import logging
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def get_connection(config: dict):
    """
    Get a psycopg2 connection using config parameters.

    Args:
        config: Dict with keys: host, port, database, user, password.

    Returns:
        psycopg2 connection object.
    """
    try:
        conn = psycopg2.connect(
            host=config.get("host", "localhost"),
            port=config.get("port", 5432),
            database=config.get("database", "financial_warehouse"),
            user=config.get("user", "pipeline_user"),
            password=config.get("password", "pipeline_pass"),
        )
        conn.autocommit = False
        logger.info(
            f"Connected to PostgreSQL at {config.get('host')}:"
            f"{config.get('port')}/{config.get('database')}"
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def load_raw_transactions(conn, records: list) -> int:
    """
    Load raw transaction records into raw_transactions table.

    Args:
        conn: Active psycopg2 connection.
        records: List of transaction dicts.

    Returns:
        Number of records inserted.
    """
    if not records:
        return 0

    sql = """
        INSERT INTO raw_transactions
            (transaction_id, account_id, transaction_type, amount,
             currency, timestamp, merchant_id, location, status, is_flagged)
        VALUES %s
        ON CONFLICT (transaction_id) DO NOTHING
    """

    values = [
        (
            r["transaction_id"],
            r["account_id"],
            r["transaction_type"],
            r["amount"],
            r.get("currency", "USD"),
            r["timestamp"],
            r.get("merchant_id"),
            r.get("location"),
            r.get("status"),
            r.get("is_flagged", False),
        )
        for r in records
    ]

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=1000)
            inserted = cur.rowcount
        conn.commit()
        logger.info(f"Loaded {inserted} raw transactions.")
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading raw transactions: {e}")
        raise


def load_processed_transactions(conn, rows: list) -> int:
    """
    Load processed (transformed) transaction records into processed_transactions table.

    Args:
        conn: Active psycopg2 connection.
        rows: List of dicts or Row objects with processed fields.

    Returns:
        Number of records inserted.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO processed_transactions
            (transaction_id, account_id, transaction_type, amount,
             transaction_date, transaction_hour, amount_bucket,
             is_suspicious, risk_score)
        VALUES %s
        ON CONFLICT (transaction_id) DO NOTHING
    """

    values = []
    for r in rows:
        # Support both dict and Row access
        if hasattr(r, "asDict"):
            r = r.asDict()
        values.append((
            r["transaction_id"],
            r["account_id"],
            r["transaction_type"],
            r["amount"],
            r["transaction_date"],
            r.get("transaction_hour"),
            r.get("amount_bucket"),
            r.get("is_suspicious", False),
            r.get("risk_score"),
        ))

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=1000)
            inserted = cur.rowcount
        conn.commit()
        logger.info(f"Loaded {inserted} processed transactions.")
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading processed transactions: {e}")
        raise


def load_daily_settlements(conn, rows: list) -> int:
    """
    Load daily settlement aggregation rows into daily_settlements table.

    Args:
        conn: Active psycopg2 connection.
        rows: List of dicts or Row objects with settlement fields.

    Returns:
        Number of records inserted.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO daily_settlements
            (settlement_date, transaction_type, total_amount, avg_amount,
             transaction_count, flagged_count, running_total, day_over_day_change)
        VALUES %s
        ON CONFLICT (settlement_date, transaction_type) DO UPDATE SET
            total_amount = EXCLUDED.total_amount,
            avg_amount = EXCLUDED.avg_amount,
            transaction_count = EXCLUDED.transaction_count,
            flagged_count = EXCLUDED.flagged_count,
            running_total = EXCLUDED.running_total,
            day_over_day_change = EXCLUDED.day_over_day_change
    """

    values = []
    for r in rows:
        if hasattr(r, "asDict"):
            r = r.asDict()
        values.append((
            r["transaction_date"],
            r["transaction_type"],
            r.get("total_amount"),
            r.get("avg_amount"),
            r.get("transaction_count"),
            r.get("flagged_count"),
            r.get("running_total"),
            r.get("day_over_day_change"),
        ))

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=500)
            inserted = cur.rowcount
        conn.commit()
        logger.info(f"Loaded {inserted} daily settlement rows.")
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading daily settlements: {e}")
        raise


def load_anomaly_alerts(conn, rows: list) -> int:
    """
    Load anomaly detection results into anomaly_alerts table.

    Args:
        conn: Active psycopg2 connection.
        rows: List of dicts or Row objects with alert fields.

    Returns:
        Number of records inserted.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO anomaly_alerts
            (account_id, alert_type, alert_description,
             transaction_count, total_amount)
        VALUES %s
    """

    values = []
    for r in rows:
        if hasattr(r, "asDict"):
            r = r.asDict()
        values.append((
            r["account_id"],
            r["alert_type"],
            r.get("alert_description"),
            r.get("transaction_count"),
            r.get("total_amount"),
        ))

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=500)
            inserted = cur.rowcount
        conn.commit()
        logger.info(f"Loaded {inserted} anomaly alerts.")
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading anomaly alerts: {e}")
        raise


def log_data_quality(
    conn,
    batch_id: Optional[str],
    total_records: int,
    valid_records: int,
    invalid_records: int,
    error_breakdown: dict,
) -> None:
    """
    Insert a data quality log entry.

    Args:
        conn: Active psycopg2 connection.
        batch_id: UUID of the batch.
        total_records: Total records in the batch.
        valid_records: Number of records that passed validation.
        invalid_records: Number of records that failed validation.
        error_breakdown: Dict mapping error category to count.
    """
    if not batch_id:
        batch_id = str(uuid.uuid4())

    sql = """
        INSERT INTO data_quality_log
            (batch_id, total_records, valid_records, invalid_records, error_breakdown)
        VALUES (%s, %s, %s, %s, %s)
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql, (
                batch_id,
                total_records,
                valid_records,
                invalid_records,
                json.dumps(error_breakdown),
            ))
        conn.commit()
        logger.info(
            f"Logged data quality — batch {batch_id}: "
            f"{valid_records}/{total_records} valid"
        )
    except Exception as e:
        conn.rollback()
        logger.error(f"Error logging data quality: {e}")
        raise
