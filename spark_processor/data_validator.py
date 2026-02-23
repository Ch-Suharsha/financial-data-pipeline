"""
Data Validator for Financial Transactions

Validates incoming transaction records against business rules
before they enter the processing pipeline.
"""

import re
import logging
from datetime import datetime
from typing import Tuple, List, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Regex patterns
UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)
ACCOUNT_ID_PATTERN = re.compile(r"^ACC_\d{5}$")
CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")

VALID_TRANSACTION_TYPES = {
    "WAGER_PLACEMENT",
    "WAGER_SETTLEMENT",
    "REFUND",
    "ACCOUNT_DEPOSIT",
    "ACCOUNT_WITHDRAWAL",
    "FEE_CHARGE",
}


def validate_transaction(record: dict) -> Tuple[bool, List[str]]:
    """
    Validate a single transaction record against business rules.

    Args:
        record: A transaction dict with expected fields.

    Returns:
        Tuple of (is_valid, list_of_error_messages).
        If is_valid is True, errors will be empty.
    """
    errors = []

    # 1. transaction_id must be non-null and valid UUID format
    tid = record.get("transaction_id")
    if not tid:
        errors.append("transaction_id is missing or null")
    elif not UUID_PATTERN.match(str(tid)):
        errors.append(f"transaction_id is not a valid UUID: {tid}")

    # 2. amount must be positive and less than 100,000
    amount = record.get("amount")
    if amount is None:
        errors.append("amount is missing or null")
    else:
        try:
            amount = float(amount)
            if amount <= 0:
                errors.append(f"amount must be positive, got {amount}")
            if amount >= 100000:
                errors.append(f"amount exceeds maximum (100,000): {amount}")
        except (ValueError, TypeError):
            errors.append(f"amount is not a valid number: {amount}")

    # 3. account_id must match pattern ACC_XXXXX
    account_id = record.get("account_id")
    if not account_id:
        errors.append("account_id is missing or null")
    elif not ACCOUNT_ID_PATTERN.match(str(account_id)):
        errors.append(f"account_id does not match ACC_XXXXX pattern: {account_id}")

    # 4. timestamp must be valid ISO datetime and not in the future
    timestamp_str = record.get("timestamp")
    if not timestamp_str:
        errors.append("timestamp is missing or null")
    else:
        try:
            ts = datetime.fromisoformat(str(timestamp_str))
            if ts > datetime.utcnow():
                errors.append(f"timestamp is in the future: {timestamp_str}")
        except (ValueError, TypeError):
            errors.append(f"timestamp is not valid ISO format: {timestamp_str}")

    # 5. transaction_type must be one of the 6 valid types
    txn_type = record.get("transaction_type")
    if not txn_type:
        errors.append("transaction_type is missing or null")
    elif txn_type not in VALID_TRANSACTION_TYPES:
        errors.append(f"transaction_type is invalid: {txn_type}")

    # 6. currency must be a valid 3-letter code
    currency = record.get("currency")
    if not currency:
        errors.append("currency is missing or null")
    elif not CURRENCY_PATTERN.match(str(currency)):
        errors.append(f"currency is not a valid 3-letter code: {currency}")

    is_valid = len(errors) == 0
    return is_valid, errors


def validate_batch(
    records: list,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Validate a batch of transaction records.

    Args:
        records: List of transaction dicts.

    Returns:
        Tuple of (valid_records, invalid_records, validation_report).
        - valid_records: list of records that passed all checks.
        - invalid_records: list of dicts with 'record' and 'errors' keys.
        - validation_report: summary dict with counts and error breakdown.
    """
    valid_records = []
    invalid_records = []
    error_breakdown: Dict[str, int] = {}

    for record in records:
        is_valid, errors = validate_transaction(record)

        if is_valid:
            valid_records.append(record)
        else:
            invalid_records.append({"record": record, "errors": errors})
            for error_msg in errors:
                # Extract error category (first word before "is" or "must" or "does")
                category = error_msg.split(" ")[0]
                error_breakdown[category] = error_breakdown.get(category, 0) + 1

            logger.debug(
                f"Invalid record {record.get('transaction_id', '?')}: "
                f"{'; '.join(errors)}"
            )

    validation_report = {
        "total": len(records),
        "valid": len(valid_records),
        "invalid": len(invalid_records),
        "valid_rate": round(len(valid_records) / max(len(records), 1) * 100, 2),
        "error_breakdown": error_breakdown,
    }

    logger.info(
        f"Validation complete — {validation_report['valid']}/{validation_report['total']} "
        f"valid ({validation_report['valid_rate']}%)"
    )

    if invalid_records:
        logger.warning(
            f"  {len(invalid_records)} invalid records. "
            f"Error breakdown: {error_breakdown}"
        )

    return valid_records, invalid_records, validation_report
