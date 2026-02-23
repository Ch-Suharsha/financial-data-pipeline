"""
Tests for spark_processor/data_validator.py
"""

import pytest
import uuid
from datetime import datetime, timedelta
from spark_processor.data_validator import validate_transaction, validate_batch


def _make_valid_record(**overrides) -> dict:
    """Create a valid transaction record, optionally overriding fields."""
    record = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": "ACC_00001",
        "transaction_type": "ACCOUNT_DEPOSIT",
        "amount": 250.00,
        "currency": "USD",
        "timestamp": (datetime.utcnow() - timedelta(hours=1)).isoformat(),
        "merchant_id": "MER_0001",
        "location": "New York",
        "status": "COMPLETED",
        "is_flagged": False,
    }
    record.update(overrides)
    return record


class TestValidateTransaction:
    """Tests for validate_transaction()."""

    def test_valid_record_passes(self):
        record = _make_valid_record()
        is_valid, errors = validate_transaction(record)
        assert is_valid is True
        assert errors == []

    def test_null_transaction_id_fails(self):
        record = _make_valid_record(transaction_id=None)
        is_valid, errors = validate_transaction(record)
        assert is_valid is False
        assert any("transaction_id" in e for e in errors)

    def test_invalid_uuid_fails(self):
        record = _make_valid_record(transaction_id="not-a-uuid")
        is_valid, errors = validate_transaction(record)
        assert is_valid is False
        assert any("UUID" in e for e in errors)

    def test_negative_amount_fails(self):
        record = _make_valid_record(amount=-50.00)
        is_valid, errors = validate_transaction(record)
        assert is_valid is False
        assert any("positive" in e for e in errors)

    def test_zero_amount_fails(self):
        record = _make_valid_record(amount=0)
        is_valid, errors = validate_transaction(record)
        assert is_valid is False

    def test_excessive_amount_fails(self):
        record = _make_valid_record(amount=150000.00)
        is_valid, errors = validate_transaction(record)
        assert is_valid is False
        assert any("100,000" in e for e in errors)

    def test_invalid_account_id_fails(self):
        record = _make_valid_record(account_id="WRONG_FORMAT")
        is_valid, errors = validate_transaction(record)
        assert is_valid is False
        assert any("ACC_XXXXX" in e for e in errors)

    def test_future_timestamp_fails(self):
        future = (datetime.utcnow() + timedelta(days=10)).isoformat()
        record = _make_valid_record(timestamp=future)
        is_valid, errors = validate_transaction(record)
        assert is_valid is False
        assert any("future" in e for e in errors)

    def test_invalid_timestamp_format_fails(self):
        record = _make_valid_record(timestamp="not-a-timestamp")
        is_valid, errors = validate_transaction(record)
        assert is_valid is False

    def test_invalid_transaction_type_fails(self):
        record = _make_valid_record(transaction_type="INVALID_TYPE")
        is_valid, errors = validate_transaction(record)
        assert is_valid is False
        assert any("invalid" in e.lower() for e in errors)

    def test_invalid_currency_fails(self):
        record = _make_valid_record(currency="TOOLONG")
        is_valid, errors = validate_transaction(record)
        assert is_valid is False
        assert any("currency" in e for e in errors)

    def test_multiple_errors_reported(self):
        record = _make_valid_record(
            transaction_id=None,
            amount=-100,
            account_id="bad",
        )
        is_valid, errors = validate_transaction(record)
        assert is_valid is False
        assert len(errors) >= 3


class TestValidateBatch:
    """Tests for validate_batch()."""

    def test_all_valid_batch(self):
        records = [_make_valid_record() for _ in range(10)]
        valid, invalid, report = validate_batch(records)
        assert len(valid) == 10
        assert len(invalid) == 0
        assert report["total"] == 10
        assert report["valid"] == 10
        assert report["valid_rate"] == 100.0

    def test_mixed_batch(self):
        records = [_make_valid_record() for _ in range(8)]
        records.append(_make_valid_record(transaction_id=None))
        records.append(_make_valid_record(amount=-1))

        valid, invalid, report = validate_batch(records)
        assert len(valid) == 8
        assert len(invalid) == 2
        assert report["total"] == 10
        assert report["invalid"] == 2

    def test_validation_report_has_error_breakdown(self):
        records = [
            _make_valid_record(transaction_id=None),
            _make_valid_record(amount=-5),
        ]
        _, _, report = validate_batch(records)
        assert "error_breakdown" in report
        assert len(report["error_breakdown"]) > 0

    def test_empty_batch(self):
        valid, invalid, report = validate_batch([])
        assert len(valid) == 0
        assert report["total"] == 0
        assert report["valid_rate"] == 0.0
