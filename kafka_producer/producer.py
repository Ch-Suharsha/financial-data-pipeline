"""
Kafka Producer for Financial Transactions

Sends generated transaction records to a Kafka topic
with JSON serialization, retry logic, and automatic topic creation.
"""

import json
import time
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

from data_generator.transaction_generator import generate_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def create_topic_if_not_exists(
    bootstrap_servers: str = "localhost:9092",
    topic_name: str = "financial_transactions",
    num_partitions: int = 3,
    replication_factor: int = 1,
) -> None:
    """
    Create the Kafka topic if it doesn't already exist.

    Args:
        bootstrap_servers: Kafka broker address.
        topic_name: Name of the topic to create.
        num_partitions: Number of partitions for the topic.
        replication_factor: Replication factor for the topic.
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        logger.info(f"Created topic '{topic_name}' with {num_partitions} partitions.")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists.")
    except NoBrokersAvailable:
        logger.error("No Kafka brokers available. Is Kafka running?")
        raise
    except Exception as e:
        logger.error(f"Error creating topic: {e}")
        raise


def get_producer(bootstrap_servers: str = "localhost:9092") -> KafkaProducer:
    """
    Create and return a KafkaProducer instance with JSON serialization.

    Args:
        bootstrap_servers: Kafka broker address.

    Returns:
        Configured KafkaProducer instance.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        retry_backoff_ms=500,
    )


def send_transactions(
    transactions: list,
    topic: str = "financial_transactions",
    bootstrap_servers: str = "localhost:9092",
    max_retries: int = 3,
) -> int:
    """
    Send a list of transaction records to the Kafka topic.

    Args:
        transactions: List of transaction dicts to send.
        topic: Kafka topic name.
        bootstrap_servers: Kafka broker address.
        max_retries: Maximum number of retry attempts on failure.

    Returns:
        Number of records successfully sent.
    """
    producer = get_producer(bootstrap_servers)
    sent_count = 0
    failed_count = 0

    for i, record in enumerate(transactions):
        retries = 0
        while retries <= max_retries:
            try:
                producer.send(topic, value=record)
                sent_count += 1
                break
            except Exception as e:
                retries += 1
                if retries > max_retries:
                    logger.error(
                        f"Failed to send record {record.get('transaction_id', '?')} "
                        f"after {max_retries} retries: {e}"
                    )
                    failed_count += 1
                else:
                    logger.warning(
                        f"Retry {retries}/{max_retries} for record "
                        f"{record.get('transaction_id', '?')}: {e}"
                    )
                    time.sleep(0.1 * retries)

        if (i + 1) % 1000 == 0:
            logger.info(f"  Sent {i + 1}/{len(transactions)} records...")

    producer.flush()
    producer.close()

    logger.info(
        f"Send complete — {sent_count} sent, {failed_count} failed "
        f"out of {len(transactions)} total."
    )
    return sent_count


if __name__ == "__main__":
    BOOTSTRAP_SERVERS = "localhost:9092"
    TOPIC = "financial_transactions"
    TEST_BATCH_SIZE = 10000

    print("=" * 60)
    print("Kafka Producer — Test Run")
    print("=" * 60)

    # Step 1: Create topic
    print("\n[1/3] Creating topic if needed...")
    create_topic_if_not_exists(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic_name=TOPIC,
    )

    # Step 2: Generate test transactions
    print(f"[2/3] Generating {TEST_BATCH_SIZE} test transactions...")
    transactions = generate_batch(batch_size=TEST_BATCH_SIZE)
    print(f"  Generated {len(transactions)} records.")

    # Step 3: Send to Kafka
    print(f"[3/3] Sending to Kafka topic '{TOPIC}'...")
    start_time = time.time()
    sent = send_transactions(
        transactions=transactions,
        topic=TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )
    elapsed = time.time() - start_time

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Results:")
    print(f"  Records sent:     {sent}")
    print(f"  Time taken:       {elapsed:.2f}s")
    print(f"  Throughput:       {sent / elapsed:.0f} records/sec")
    print(f"{'=' * 60}")
