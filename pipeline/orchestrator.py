"""
Pipeline Orchestrator

Main script that runs the financial transaction pipeline end-to-end:
  1. Load config
  2. Check Docker services
  3. Generate transaction data
  4. Send data to Kafka
  5. Process with Spark
  6. Validate data
  7. Transform and enrich
  8. Load into PostgreSQL warehouse
  9. Run anomaly detection
  10. Log data quality metrics
  11. Print summary report

Usage:
    python -m pipeline.orchestrator --mode test          # 100 records
    python -m pipeline.orchestrator --mode full           # 500K records
    python -m pipeline.orchestrator --mode test --skip-kafka  # skip Kafka, use in-memory
"""

import os
import sys
import json
import time
import uuid
import socket
import argparse
import logging
from datetime import datetime

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("orchestrator")


def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    """Load pipeline configuration from YAML file."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    logger.info(f"Loaded config from {config_path}")
    return config


def check_service(host: str, port: int, name: str, timeout: float = 2.0) -> bool:
    """Check if a service is reachable at host:port."""
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        logger.info(f"  ✓ {name} is reachable at {host}:{port}")
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        logger.warning(f"  ✗ {name} is NOT reachable at {host}:{port}")
        return False


def check_infrastructure(config: dict) -> dict:
    """Check that Docker services (Kafka, PostgreSQL) are running."""
    logger.info("Checking infrastructure services...")

    kafka_servers = config["kafka"]["bootstrap_servers"]
    kafka_host = kafka_servers.split(":")[0]
    kafka_port = int(kafka_servers.split(":")[1])

    pg_host = config["postgres"]["host"]
    pg_port = config["postgres"]["port"]

    results = {
        "kafka": check_service(kafka_host, kafka_port, "Kafka"),
        "postgres": check_service(pg_host, pg_port, "PostgreSQL"),
    }

    return results


def run_pipeline(mode: str = "test", skip_kafka: bool = False):
    """
    Execute the full pipeline.

    Args:
        mode: 'test' (100 records) or 'full' (500K records).
        skip_kafka: If True, skip Kafka and process data in-memory.
    """
    pipeline_start = time.time()
    run_id = str(uuid.uuid4())[:8]

    logger.info("=" * 70)
    logger.info(f"Financial Transaction Pipeline — Run {run_id}")
    logger.info(f"  Mode:        {mode}")
    logger.info(f"  Skip Kafka:  {skip_kafka}")
    logger.info(f"  Started at:  {datetime.now().isoformat()}")
    logger.info("=" * 70)

    # ── Step 1: Load config ──────────────────────────────────
    step_start = time.time()
    config = load_config()

    if mode == "test":
        num_records = 100
        batch_size = 100
    else:
        num_records = config["generator"]["num_records"]
        batch_size = config["generator"]["batch_size"]

    logger.info(f"[Step 1/10] Config loaded. Records: {num_records}, Batch: {batch_size}")
    logger.info(f"  ⏱  {time.time() - step_start:.2f}s")

    # ── Step 2: Check infrastructure ─────────────────────────
    step_start = time.time()
    infra = check_infrastructure(config)

    if not skip_kafka and not infra["kafka"]:
        logger.error("Kafka is not available. Use --skip-kafka or start Docker services.")
        sys.exit(1)

    if not infra["postgres"]:
        logger.warning("PostgreSQL is not available. Warehouse loading will be skipped.")

    logger.info(f"[Step 2/10] Infrastructure check complete.")
    logger.info(f"  ⏱  {time.time() - step_start:.2f}s")

    # ── Step 3: Generate transaction data ────────────────────
    step_start = time.time()
    from data_generator.transaction_generator import generate_batch

    all_records = generate_batch(
        batch_size=num_records,
        anomaly_rate=config["generator"]["anomaly_rate"],
        min_amount=config["generator"]["min_amount"],
        max_amount=config["generator"]["max_amount"],
        accounts_count=config["generator"]["accounts_count"],
    )
    logger.info(f"[Step 3/10] Generated {len(all_records)} records.")
    logger.info(f"  ⏱  {time.time() - step_start:.2f}s")

    # ── Step 4: Send to Kafka (unless skipped) ───────────────
    if not skip_kafka:
        step_start = time.time()
        try:
            from kafka_producer.producer import create_topic_if_not_exists, send_transactions

            create_topic_if_not_exists(
                bootstrap_servers=config["kafka"]["bootstrap_servers"],
                topic_name=config["kafka"]["topic"],
            )

            sent = send_transactions(
                transactions=all_records,
                topic=config["kafka"]["topic"],
                bootstrap_servers=config["kafka"]["bootstrap_servers"],
            )
            logger.info(f"[Step 4/10] Sent {sent} records to Kafka.")
        except Exception as e:
            logger.error(f"[Step 4/10] Kafka send failed: {e}")
            logger.info("  Falling back to in-memory processing...")
            skip_kafka = True
        logger.info(f"  ⏱  {time.time() - step_start:.2f}s")
    else:
        logger.info("[Step 4/10] Skipped (--skip-kafka).")

    # ── Step 5: Validate data ────────────────────────────────
    step_start = time.time()
    from spark_processor.data_validator import validate_batch

    valid_records, invalid_records, validation_report = validate_batch(all_records)

    logger.info(
        f"[Step 5/10] Validation: {validation_report['valid']}/{validation_report['total']} "
        f"valid ({validation_report['valid_rate']}%)"
    )
    logger.info(f"  ⏱  {time.time() - step_start:.2f}s")

    # ── Step 6: Spark processing ─────────────────────────────
    step_start = time.time()
    try:
        from spark_processor.batch_processor import (
            get_spark_session,
            process_batch,
        )

        spark = get_spark_session(
            app_name=config["spark"]["app_name"],
            master=config["spark"]["master"],
        )

        result = process_batch(
            spark=spark,
            records=valid_records,
        )

        metrics = result["metrics"]
        transformed_df = result["transformed_df"]
        settlements_df = result["settlements_df"]
        anomalies_df = result["anomalies_df"]

        logger.info(
            f"[Step 6/10] Spark processing done — "
            f"{metrics['records_transformed']} transformed, "
            f"{metrics['anomalies_detected']} anomalies."
        )
    except Exception as e:
        logger.error(f"[Step 6/10] Spark processing failed: {e}")
        logger.info("  Pipeline will continue without Spark results.")
        transformed_df = None
        settlements_df = None
        anomalies_df = None
        metrics = {}
    logger.info(f"  ⏱  {time.time() - step_start:.2f}s")

    # ── Step 7: Load into PostgreSQL ─────────────────────────
    pg_loaded = False
    if infra.get("postgres") and transformed_df is not None:
        step_start = time.time()
        try:
            from warehouse.loader import (
                get_connection,
                load_raw_transactions,
                load_processed_transactions,
                load_daily_settlements,
                load_anomaly_alerts,
                log_data_quality,
            )

            conn = get_connection(config["postgres"])

            # Load raw
            raw_count = load_raw_transactions(conn, all_records)

            # Load processed
            processed_rows = [row.asDict() for row in transformed_df.collect()]
            proc_count = load_processed_transactions(conn, processed_rows)

            # Load settlements
            settlement_rows = [row.asDict() for row in settlements_df.collect()]
            settle_count = load_daily_settlements(conn, settlement_rows)

            # Load anomalies
            anomaly_rows = [row.asDict() for row in anomalies_df.collect()]
            anomaly_count = load_anomaly_alerts(conn, anomaly_rows)

            # Log data quality
            log_data_quality(
                conn=conn,
                batch_id=run_id,
                total_records=validation_report["total"],
                valid_records=validation_report["valid"],
                invalid_records=validation_report["invalid"],
                error_breakdown=validation_report["error_breakdown"],
            )

            conn.close()
            pg_loaded = True

            logger.info(
                f"[Step 7/10] PostgreSQL loaded — "
                f"raw: {raw_count}, processed: {proc_count}, "
                f"settlements: {settle_count}, anomalies: {anomaly_count}"
            )
        except Exception as e:
            logger.error(f"[Step 7/10] PostgreSQL loading failed: {e}")
        logger.info(f"  ⏱  {time.time() - step_start:.2f}s")
    else:
        logger.info("[Step 7/10] Skipped (PostgreSQL not available or no Spark results).")

    # ── Step 8-10: Summary report ────────────────────────────
    elapsed = time.time() - pipeline_start

    logger.info("")
    logger.info("=" * 70)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 70)
    logger.info(f"  Run ID:                  {run_id}")
    logger.info(f"  Mode:                    {mode}")
    logger.info(f"  Total time:              {elapsed:.2f}s")
    logger.info(f"  Records generated:       {len(all_records)}")
    logger.info(f"  Records valid:           {validation_report['valid']}")
    logger.info(f"  Records invalid:         {validation_report['invalid']}")
    logger.info(f"  Records transformed:     {metrics.get('records_transformed', 'N/A')}")
    logger.info(f"  Settlement rows:         {metrics.get('settlement_rows', 'N/A')}")
    logger.info(f"  Anomalies detected:      {metrics.get('anomalies_detected', 'N/A')}")
    logger.info(f"  Loaded to PostgreSQL:    {'Yes' if pg_loaded else 'No'}")
    logger.info(f"  Kafka used:              {'No' if skip_kafka else 'Yes'}")
    logger.info("=" * 70)

    # Clean up Spark
    if transformed_df is not None:
        try:
            spark.stop()
        except Exception:
            pass

    return {
        "run_id": run_id,
        "mode": mode,
        "elapsed_seconds": round(elapsed, 2),
        "records_generated": len(all_records),
        "validation_report": validation_report,
        "processing_metrics": metrics,
        "pg_loaded": pg_loaded,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Financial Transaction Pipeline Orchestrator"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "test"],
        default="test",
        help="Pipeline mode: 'test' (100 records) or 'full' (500K records)",
    )
    parser.add_argument(
        "--skip-kafka",
        action="store_true",
        default=False,
        help="Skip Kafka and process data in-memory",
    )

    args = parser.parse_args()
    result = run_pipeline(mode=args.mode, skip_kafka=args.skip_kafka)

    print(f"\nPipeline finished. Run ID: {result['run_id']}")
