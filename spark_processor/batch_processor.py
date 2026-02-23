"""
PySpark Batch Processor for Financial Transactions

Core processing engine that reads transaction data, validates, transforms,
detects anomalies, and writes results to the warehouse.

Uses BOTH DataFrame API and RDD operations as required.
"""

import json
import logging
import time
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
)
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ---------- Schema Definition ----------

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("timestamp", StringType(), False),
    StructField("merchant_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("status", StringType(), True),
    StructField("is_flagged", BooleanType(), True),
])


# ---------- Spark Session ----------

def get_spark_session(
    app_name: str = "FinancialTransactionProcessor",
    master: str = "local[*]",
) -> SparkSession:
    """Create or get an existing SparkSession."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------- Data Reading ----------

def read_from_kafka(
    spark: SparkSession,
    bootstrap_servers: str = "localhost:9092",
    topic: str = "financial_transactions",
) -> DataFrame:
    """
    Read batch data from Kafka topic and parse JSON payloads into a structured DataFrame.

    Args:
        spark: Active SparkSession.
        bootstrap_servers: Kafka broker address.
        topic: Kafka topic to read from.

    Returns:
        DataFrame with parsed transaction columns.
    """
    logger.info(f"Reading from Kafka topic '{topic}'...")

    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON value column using the defined schema
    parsed_df = (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
    )

    logger.info(f"Read {parsed_df.count()} records from Kafka.")
    return parsed_df


def read_from_json(spark: SparkSession, json_path: str) -> DataFrame:
    """
    Read transaction data from a local JSON file (for development/testing).

    Args:
        spark: Active SparkSession.
        json_path: Path to the JSON file.

    Returns:
        DataFrame with transaction columns.
    """
    logger.info(f"Reading from JSON file '{json_path}'...")
    df = spark.read.schema(TRANSACTION_SCHEMA).json(json_path)
    logger.info(f"Read {df.count()} records from JSON.")
    return df


def create_dataframe_from_records(
    spark: SparkSession, records: list
) -> DataFrame:
    """
    Create a DataFrame directly from a list of transaction dicts.

    Args:
        spark: Active SparkSession.
        records: List of transaction dicts.

    Returns:
        DataFrame with transaction columns.
    """
    rows = []
    for r in records:
        rows.append(Row(
            transaction_id=r["transaction_id"],
            account_id=r["account_id"],
            transaction_type=r["transaction_type"],
            amount=float(r["amount"]),
            currency=r.get("currency", "USD"),
            timestamp=r["timestamp"],
            merchant_id=r.get("merchant_id"),
            location=r.get("location"),
            status=r.get("status"),
            is_flagged=r.get("is_flagged", False),
        ))
    return spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)


# ---------- Transformations (DataFrame API) ----------

def transform_transactions(df: DataFrame) -> DataFrame:
    """
    Apply business transformations using DataFrame API and RDD operations.

    Derived columns added:
    - transaction_date: DATE extracted from timestamp
    - transaction_hour: INTEGER hour extracted from timestamp
    - amount_bucket: 'small' (<100), 'medium' (100-1000), 'large' (>1000)
    - is_suspicious: BOOLEAN based on rules
    - risk_score: FLOAT 0-1 computed via RDD map operation

    Args:
        df: Input DataFrame with raw transaction columns.

    Returns:
        Transformed DataFrame with all derived columns.
    """
    logger.info("Transforming transactions using DataFrame API...")

    # --- DataFrame API transformations ---
    transformed = (
        df
        # Cast timestamp string to timestamp type
        .withColumn("ts", F.to_timestamp(F.col("timestamp")))

        # Derived: transaction_date
        .withColumn("transaction_date", F.to_date(F.col("ts")))

        # Derived: transaction_hour
        .withColumn("transaction_hour", F.hour(F.col("ts")))

        # Derived: amount_bucket
        .withColumn(
            "amount_bucket",
            F.when(F.col("amount") < 100, "small")
            .when(F.col("amount") <= 1000, "medium")
            .otherwise("large"),
        )

        # Derived: is_suspicious
        # Rules: amount > 5000, OR hour between 1-5 AM, OR is_flagged
        .withColumn(
            "is_suspicious",
            (F.col("amount") > 5000)
            | (F.col("transaction_hour").between(1, 5))
            | (F.col("is_flagged") == True),  # noqa: E712
        )

        # Drop the intermediate timestamp column
        .drop("ts")
    )

    # --- RDD operation: Calculate risk_score ---
    # Convert DataFrame → RDD, apply map, convert back
    logger.info("Computing risk_score via RDD map operation...")

    def compute_risk_score(row: Row) -> Row:
        """
        Compute a risk score (0.0 - 1.0) based on transaction characteristics.

        Scoring heuristic:
        - Base score: 0.1
        - +0.3 if amount > 5000
        - +0.2 if transaction_hour between 1-5 AM
        - +0.2 if is_flagged
        - +0.1 if amount_bucket == 'large'
        - +0.1 if status == 'FAILED'
        """
        score = 0.1

        if row.amount and row.amount > 5000:
            score += 0.3
        if row.transaction_hour is not None and 1 <= row.transaction_hour <= 5:
            score += 0.2
        if row.is_flagged:
            score += 0.2
        if row.amount_bucket == "large":
            score += 0.1
        if row.status == "FAILED":
            score += 0.1

        score = min(score, 1.0)

        return Row(
            transaction_id=row.transaction_id,
            account_id=row.account_id,
            transaction_type=row.transaction_type,
            amount=row.amount,
            currency=row.currency,
            timestamp=row.timestamp,
            merchant_id=row.merchant_id,
            location=row.location,
            status=row.status,
            is_flagged=row.is_flagged,
            transaction_date=row.transaction_date,
            transaction_hour=row.transaction_hour,
            amount_bucket=row.amount_bucket,
            is_suspicious=row.is_suspicious,
            risk_score=round(score, 2),
        )

    rdd_with_risk = transformed.rdd.map(compute_risk_score)
    result_df = rdd_with_risk.toDF()

    logger.info(f"Transformation complete. {result_df.count()} records processed.")
    return result_df


# ---------- Aggregation (Window Functions) ----------

def aggregate_daily_settlements(df: DataFrame) -> DataFrame:
    """
    Aggregate transactions by date and type with running totals and day-over-day change.

    Uses window functions: SUM OVER, LAG.

    Args:
        df: Transformed DataFrame with transaction_date column.

    Returns:
        Aggregated DataFrame with settlement metrics.
    """
    logger.info("Aggregating daily settlements...")

    # Group by date and transaction_type
    agg_df = (
        df.groupBy("transaction_date", "transaction_type")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.count("*").alias("transaction_count"),
            F.sum(F.when(F.col("is_flagged") == True, 1).otherwise(0)).alias("flagged_count"),  # noqa: E712
        )
    )

    # Window for running totals and day-over-day change
    window_running = (
        Window
        .partitionBy("transaction_type")
        .orderBy("transaction_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    window_lag = (
        Window
        .partitionBy("transaction_type")
        .orderBy("transaction_date")
    )

    settlements = (
        agg_df
        .withColumn("running_total", F.sum("total_amount").over(window_running))
        .withColumn(
            "day_over_day_change",
            F.round(
                F.col("total_amount") - F.lag("total_amount", 1).over(window_lag),
                2,
            ),
        )
        .orderBy("transaction_date", "transaction_type")
    )

    logger.info(f"Daily settlements aggregated: {settlements.count()} rows.")
    return settlements


# ---------- Anomaly Detection (Window Functions) ----------

def detect_anomalies(df: DataFrame) -> DataFrame:
    """
    Detect anomalous account behavior using window functions.

    Anomaly rules:
    1. > 10 transactions in a 1-hour window per account
    2. Total daily amount > 50,000 per account
    3. Rapid successive transactions (< 30 seconds apart) from same account

    Args:
        df: Transformed DataFrame with timestamp and account_id.

    Returns:
        DataFrame of anomaly alerts (account_id, alert_type, description, etc.)
    """
    logger.info("Running anomaly detection...")

    alerts = []

    # --- Rule 1: > 10 transactions in 1-hour window ---
    window_1h = (
        Window
        .partitionBy("account_id")
        .orderBy(F.col("timestamp").cast("timestamp").cast("long"))
        .rangeBetween(-3600, 0)
    )

    hourly_counts = (
        df.withColumn("txn_count_1h", F.count("*").over(window_1h))
        .filter(F.col("txn_count_1h") > 10)
        .groupBy("account_id")
        .agg(
            F.max("txn_count_1h").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
        )
        .withColumn("alert_type", F.lit("HIGH_FREQUENCY"))
        .withColumn(
            "alert_description",
            F.concat(
                F.lit("Account had "),
                F.col("transaction_count").cast("string"),
                F.lit(" transactions in a 1-hour window"),
            ),
        )
    )
    alerts.append(hourly_counts)

    # --- Rule 2: Daily amount > 50,000 ---
    daily_totals = (
        df.withColumn("txn_date", F.to_date(F.col("timestamp")))
        .groupBy("account_id", "txn_date")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("*").alias("transaction_count"),
        )
        .filter(F.col("total_amount") > 50000)
        .drop("txn_date")
        .withColumn("alert_type", F.lit("HIGH_DAILY_AMOUNT"))
        .withColumn(
            "alert_description",
            F.concat(
                F.lit("Account exceeded $50,000 daily threshold with $"),
                F.round(F.col("total_amount"), 2).cast("string"),
            ),
        )
    )
    alerts.append(daily_totals)

    # --- Rule 3: Rapid successive transactions (< 30 seconds apart) ---
    window_by_account = (
        Window
        .partitionBy("account_id")
        .orderBy("timestamp")
    )

    rapid_txns = (
        df.withColumn("ts_epoch", F.col("timestamp").cast("timestamp").cast("long"))
        .withColumn("prev_ts_epoch", F.lag("ts_epoch", 1).over(window_by_account))
        .withColumn("time_diff_sec", F.col("ts_epoch") - F.col("prev_ts_epoch"))
        .filter(F.col("time_diff_sec").isNotNull() & (F.col("time_diff_sec") < 30))
        .groupBy("account_id")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
        )
        .withColumn("alert_type", F.lit("RAPID_SUCCESSION"))
        .withColumn(
            "alert_description",
            F.concat(
                F.lit("Account had "),
                F.col("transaction_count").cast("string"),
                F.lit(" rapid successive transactions (< 30s apart)"),
            ),
        )
    )
    alerts.append(rapid_txns)

    # Union all alert DataFrames
    all_alerts = alerts[0]
    for alert_df in alerts[1:]:
        all_alerts = all_alerts.unionByName(alert_df)

    # Select final columns
    result = all_alerts.select(
        "account_id",
        "alert_type",
        "alert_description",
        "transaction_count",
        "total_amount",
    )

    logger.info(f"Anomaly detection complete. {result.count()} alerts generated.")
    return result


# ---------- Pipeline Orchestration ----------

def process_batch(
    spark: SparkSession,
    records: list = None,
    input_path: str = None,
    use_kafka: bool = False,
    kafka_config: dict = None,
    output_path: str = None,
) -> dict:
    """
    Run the full processing pipeline: read → validate → transform → aggregate → detect → output.

    Args:
        spark: Active SparkSession.
        records: Optional list of transaction dicts (if provided, used directly).
        input_path: Optional path to JSON input file.
        use_kafka: If True, read from Kafka instead.
        kafka_config: Dict with bootstrap_servers and topic (required if use_kafka=True).
        output_path: Optional output path for parquet files.

    Returns:
        Dict with processing metrics.
    """
    start_time = time.time()
    metrics = {}

    # --- Step 1: Read data ---
    if records is not None:
        logger.info(f"Creating DataFrame from {len(records)} in-memory records...")
        df = create_dataframe_from_records(spark, records)
    elif use_kafka and kafka_config:
        df = read_from_kafka(
            spark,
            bootstrap_servers=kafka_config.get("bootstrap_servers", "localhost:9092"),
            topic=kafka_config.get("topic", "financial_transactions"),
        )
    elif input_path:
        df = read_from_json(spark, input_path)
    else:
        raise ValueError("Must provide records, input_path, or use_kafka=True")

    metrics["records_read"] = df.count()

    # --- Step 2: Transform ---
    transformed_df = transform_transactions(df)
    metrics["records_transformed"] = transformed_df.count()

    # --- Step 3: Aggregate daily settlements ---
    settlements_df = aggregate_daily_settlements(transformed_df)
    metrics["settlement_rows"] = settlements_df.count()

    # --- Step 4: Detect anomalies ---
    anomalies_df = detect_anomalies(transformed_df)
    metrics["anomalies_detected"] = anomalies_df.count()

    # --- Step 5: Write output ---
    if output_path:
        logger.info(f"Writing processed data to {output_path}...")
        transformed_df.write.mode("overwrite").parquet(f"{output_path}/processed")
        settlements_df.write.mode("overwrite").parquet(f"{output_path}/settlements")
        anomalies_df.write.mode("overwrite").parquet(f"{output_path}/anomalies")
        logger.info("Parquet output written.")

    elapsed = time.time() - start_time
    metrics["processing_time_sec"] = round(elapsed, 2)

    logger.info(
        f"Batch processing complete in {elapsed:.2f}s — "
        f"Read: {metrics['records_read']}, "
        f"Transformed: {metrics['records_transformed']}, "
        f"Settlements: {metrics['settlement_rows']}, "
        f"Anomalies: {metrics['anomalies_detected']}"
    )

    return {
        "metrics": metrics,
        "transformed_df": transformed_df,
        "settlements_df": settlements_df,
        "anomalies_df": anomalies_df,
    }
