"""
Financial Transaction Data Generator

Generates realistic simulated financial transaction records
with configurable anomaly injection for pipeline testing.
"""

import uuid
import random
import json
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Valid transaction types
TRANSACTION_TYPES = [
    "WAGER_PLACEMENT",
    "WAGER_SETTLEMENT",
    "REFUND",
    "ACCOUNT_DEPOSIT",
    "ACCOUNT_WITHDRAWAL",
    "FEE_CHARGE",
]

STATUSES = ["COMPLETED", "PENDING", "FAILED"]

# US cities for location field
US_CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
    "Indianapolis", "San Francisco", "Seattle", "Denver", "Nashville",
    "Oklahoma City", "El Paso", "Boston", "Portland", "Las Vegas",
    "Memphis", "Louisville", "Baltimore", "Milwaukee", "Albuquerque",
    "Tucson", "Fresno", "Sacramento", "Mesa", "Kansas City",
    "Atlanta", "Omaha", "Colorado Springs", "Raleigh", "Long Beach",
]


def _generate_normal_transaction(
    min_amount: float = 1.00,
    max_amount: float = 10000.00,
    accounts_count: int = 50000,
) -> dict:
    """Generate a single normal (non-anomalous) transaction record."""
    now = datetime.utcnow()
    timestamp = now - timedelta(
        days=random.randint(0, 29),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC_{random.randint(1, accounts_count):05d}",
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "amount": round(random.uniform(min_amount, max_amount), 2),
        "currency": "USD",
        "timestamp": timestamp.isoformat(),
        "merchant_id": f"MER_{random.randint(1, 500):04d}",
        "location": random.choice(US_CITIES),
        "status": random.choices(STATUSES, weights=[0.85, 0.10, 0.05], k=1)[0],
        "is_flagged": False,
    }


def _generate_anomalous_transaction(
    accounts_count: int = 50000,
) -> dict:
    """
    Generate an anomalous transaction.

    Anomaly patterns:
    - Unusually high amounts (> 50,000)
    - Transactions at unusual hours (1-5 AM)
    - Rapid successive transactions from the same account (simulated via short timestamp gaps)
    """
    anomaly_type = random.choice(["high_amount", "odd_hour", "rapid_succession"])
    now = datetime.utcnow()

    record = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC_{random.randint(1, accounts_count):05d}",
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "amount": round(random.uniform(1.00, 10000.00), 2),
        "currency": "USD",
        "timestamp": now.isoformat(),
        "merchant_id": f"MER_{random.randint(1, 500):04d}",
        "location": random.choice(US_CITIES),
        "status": random.choices(STATUSES, weights=[0.85, 0.10, 0.05], k=1)[0],
        "is_flagged": True,
    }

    if anomaly_type == "high_amount":
        record["amount"] = round(random.uniform(50000.00, 99999.99), 2)
    elif anomaly_type == "odd_hour":
        odd_hour = random.randint(1, 5)
        timestamp = now.replace(hour=odd_hour) - timedelta(days=random.randint(0, 29))
        record["timestamp"] = timestamp.isoformat()
    elif anomaly_type == "rapid_succession":
        # Cluster multiple transactions very close in time
        timestamp = now - timedelta(
            days=random.randint(0, 29),
            seconds=random.randint(0, 30),
        )
        record["timestamp"] = timestamp.isoformat()

    return record


def generate_batch(
    batch_size: int = 10000,
    anomaly_rate: float = 0.02,
    min_amount: float = 1.00,
    max_amount: float = 10000.00,
    accounts_count: int = 50000,
) -> list:
    """
    Generate a batch of transaction records.

    Args:
        batch_size: Number of records to generate in this batch.
        anomaly_rate: Fraction of records that should be anomalous (0.0 to 1.0).
        min_amount: Minimum normal transaction amount.
        max_amount: Maximum normal transaction amount.
        accounts_count: Number of unique accounts in the universe.

    Returns:
        List of transaction dicts.
    """
    records = []
    num_anomalies = int(batch_size * anomaly_rate)
    num_normal = batch_size - num_anomalies

    for _ in range(num_normal):
        records.append(
            _generate_normal_transaction(min_amount, max_amount, accounts_count)
        )

    for _ in range(num_anomalies):
        records.append(_generate_anomalous_transaction(accounts_count))

    # Shuffle so anomalies aren't clustered at the end
    random.shuffle(records)
    return records


def generate_all(
    num_records: int = 500000,
    batch_size: int = 10000,
    anomaly_rate: float = 0.02,
    min_amount: float = 1.00,
    max_amount: float = 10000.00,
    accounts_count: int = 50000,
):
    """
    Generator that yields batches of transaction records.

    Args:
        num_records: Total number of records to generate.
        batch_size: Size of each batch.
        anomaly_rate: Fraction of records that should be anomalous.
        min_amount: Minimum normal transaction amount.
        max_amount: Maximum normal transaction amount.
        accounts_count: Number of unique accounts in the universe.

    Yields:
        List of transaction dicts, one batch at a time.
    """
    generated = 0
    batch_num = 0

    while generated < num_records:
        current_batch_size = min(batch_size, num_records - generated)
        batch = generate_batch(
            batch_size=current_batch_size,
            anomaly_rate=anomaly_rate,
            min_amount=min_amount,
            max_amount=max_amount,
            accounts_count=accounts_count,
        )
        batch_num += 1
        generated += len(batch)
        print(f"  [Batch {batch_num}] Generated {len(batch)} records "
              f"(total: {generated}/{num_records})")
        yield batch


if __name__ == "__main__":
    print("=" * 60)
    print("Financial Transaction Generator — Sample Output")
    print("=" * 60)

    sample = generate_batch(batch_size=100, anomaly_rate=0.02)
    flagged = sum(1 for r in sample if r["is_flagged"])

    print(f"\nGenerated {len(sample)} records")
    print(f"Flagged (anomalous): {flagged}")
    print(f"Normal: {len(sample) - flagged}")
    print(f"\nSample record:")
    print(json.dumps(sample[0], indent=2))

    # Verify all required fields are present
    required_fields = [
        "transaction_id", "account_id", "transaction_type", "amount",
        "currency", "timestamp", "merchant_id", "location", "status",
        "is_flagged",
    ]
    for record in sample:
        for field in required_fields:
            assert field in record, f"Missing field: {field}"

    print(f"\n✓ All {len(sample)} records have all {len(required_fields)} required fields.")
