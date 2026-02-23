"""
Tests for data_generator/transaction_generator.py
"""

import pytest
from data_generator.transaction_generator import (
    generate_batch,
    generate_all,
    TRANSACTION_TYPES,
)


REQUIRED_FIELDS = [
    "transaction_id",
    "account_id",
    "transaction_type",
    "amount",
    "currency",
    "timestamp",
    "merchant_id",
    "location",
    "status",
    "is_flagged",
]


class TestGenerateBatch:
    """Tests for generate_batch()."""

    def test_returns_correct_number_of_records(self):
        batch = generate_batch(batch_size=500)
        assert len(batch) == 500

    def test_returns_correct_count_for_small_batch(self):
        batch = generate_batch(batch_size=1)
        assert len(batch) == 1

    def test_all_required_fields_present(self):
        batch = generate_batch(batch_size=100)
        for record in batch:
            for field in REQUIRED_FIELDS:
                assert field in record, f"Missing field '{field}' in record"

    def test_anomaly_rate_approximately_correct(self):
        # Use a large batch for statistical significance
        batch = generate_batch(batch_size=10000, anomaly_rate=0.02)
        flagged = sum(1 for r in batch if r["is_flagged"])
        expected = 10000 * 0.02  # 200
        # Within 1% tolerance (100-300 range)
        assert abs(flagged - expected) / 10000 <= 0.01, (
            f"Anomaly rate off: expected ~{expected}, got {flagged}"
        )

    def test_amounts_within_configured_bounds(self):
        batch = generate_batch(
            batch_size=500,
            min_amount=5.00,
            max_amount=500.00,
            anomaly_rate=0.0,  # No anomalies to avoid high-amount outliers
        )
        for record in batch:
            assert record["amount"] >= 5.00, f"Amount below min: {record['amount']}"
            assert record["amount"] <= 500.00, f"Amount above max: {record['amount']}"

    def test_transaction_types_are_valid(self):
        batch = generate_batch(batch_size=1000)
        for record in batch:
            assert record["transaction_type"] in TRANSACTION_TYPES, (
                f"Invalid type: {record['transaction_type']}"
            )

    def test_account_id_format(self):
        batch = generate_batch(batch_size=100)
        for record in batch:
            assert record["account_id"].startswith("ACC_"), (
                f"Invalid account_id: {record['account_id']}"
            )

    def test_currency_is_usd(self):
        batch = generate_batch(batch_size=100)
        for record in batch:
            assert record["currency"] == "USD"


class TestGenerateAll:
    """Tests for generate_all() generator."""

    def test_yields_correct_total_records(self):
        total = 0
        for batch in generate_all(num_records=250, batch_size=100):
            total += len(batch)
        assert total == 250

    def test_yields_correct_batch_sizes(self):
        batches = list(generate_all(num_records=250, batch_size=100))
        assert len(batches) == 3
        assert len(batches[0]) == 100
        assert len(batches[1]) == 100
        assert len(batches[2]) == 50  # Remaining
