"""
Integration Tests for the Pipeline

Tests: generate records → process through Spark → verify output schema and anomaly detection.
"""

import pytest
from data_generator.transaction_generator import generate_batch
from spark_processor.data_validator import validate_batch


class TestPipelineIntegration:
    """Integration tests for the data pipeline (non-Spark components)."""

    def test_generate_and_validate_flow(self):
        """Generate 100 records and validate them — should have high valid rate."""
        records = generate_batch(batch_size=100, anomaly_rate=0.02)
        valid, invalid, report = validate_batch(records)

        # Most records should be valid (anomalous != invalid)
        assert report["valid_rate"] >= 90.0, (
            f"Valid rate too low: {report['valid_rate']}%"
        )

    def test_all_valid_records_have_required_fields(self):
        """After validation, valid records should still have all fields."""
        records = generate_batch(batch_size=50)
        valid, _, _ = validate_batch(records)

        required = [
            "transaction_id", "account_id", "transaction_type",
            "amount", "currency", "timestamp",
        ]
        for record in valid:
            for field in required:
                assert field in record

    def test_invalid_records_have_error_details(self):
        """Invalid records from validation should include error info."""
        # Inject some bad records
        records = generate_batch(batch_size=10)
        records.append({
            "transaction_id": None,
            "account_id": "BAD",
            "transaction_type": "INVALID",
            "amount": -100,
            "currency": "TOOLONG",
            "timestamp": "not-a-date",
        })

        _, invalid, report = validate_batch(records)
        assert len(invalid) >= 1
        assert "errors" in invalid[0]
        assert len(invalid[0]["errors"]) > 0

    def test_anomaly_flags_present_in_generated_data(self):
        """Generated data with anomaly_rate > 0 should contain flagged records."""
        records = generate_batch(batch_size=1000, anomaly_rate=0.05)
        flagged_count = sum(1 for r in records if r.get("is_flagged"))
        assert flagged_count > 0, "No flagged records found"
        # Expect roughly 5% ± 2%
        assert 20 <= flagged_count <= 80, (
            f"Anomaly count out of range: {flagged_count}"
        )

    def test_validation_report_structure(self):
        """Validation report should have all expected keys."""
        records = generate_batch(batch_size=50)
        _, _, report = validate_batch(records)

        assert "total" in report
        assert "valid" in report
        assert "invalid" in report
        assert "valid_rate" in report
        assert "error_breakdown" in report
        assert isinstance(report["error_breakdown"], dict)


class TestSparkProcessing:
    """
    Spark-dependent integration tests.
    These tests require PySpark to be installed.
    """

    @pytest.fixture(scope="class")
    def spark(self):
        """Create a SparkSession for testing."""
        try:
            from spark_processor.batch_processor import get_spark_session
            spark = get_spark_session(app_name="PipelineTest", master="local[2]")
            yield spark
            spark.stop()
        except ImportError:
            pytest.skip("PySpark not installed")

    def test_create_dataframe_from_records(self, spark):
        """Test creating a DataFrame from generated records."""
        from spark_processor.batch_processor import create_dataframe_from_records

        records = generate_batch(batch_size=50, anomaly_rate=0.0)
        df = create_dataframe_from_records(spark, records)

        assert df.count() == 50
        expected_cols = {
            "transaction_id", "account_id", "transaction_type",
            "amount", "currency", "timestamp", "merchant_id",
            "location", "status", "is_flagged",
        }
        assert expected_cols.issubset(set(df.columns))

    def test_transform_adds_derived_columns(self, spark):
        """Transformed DataFrame should have derived columns."""
        from spark_processor.batch_processor import (
            create_dataframe_from_records,
            transform_transactions,
        )

        records = generate_batch(batch_size=20, anomaly_rate=0.0)
        df = create_dataframe_from_records(spark, records)
        transformed = transform_transactions(df)

        derived_cols = {
            "transaction_date", "transaction_hour",
            "amount_bucket", "is_suspicious", "risk_score",
        }
        assert derived_cols.issubset(set(transformed.columns)), (
            f"Missing columns: {derived_cols - set(transformed.columns)}"
        )

    def test_anomaly_detection_flags_patterns(self, spark):
        """Anomaly detection should produce alerts for suspicious patterns."""
        from spark_processor.batch_processor import (
            create_dataframe_from_records,
            transform_transactions,
            detect_anomalies,
        )

        # Generate records with high anomaly rate
        records = generate_batch(batch_size=200, anomaly_rate=0.10)
        df = create_dataframe_from_records(spark, records)
        transformed = transform_transactions(df)
        anomalies = detect_anomalies(transformed)

        # Schema should be correct
        expected_cols = {"account_id", "alert_type", "alert_description",
                         "transaction_count", "total_amount"}
        assert expected_cols.issubset(set(anomalies.columns))

    def test_process_batch_returns_all_outputs(self, spark):
        """process_batch should return metrics and all DataFrames."""
        from spark_processor.batch_processor import process_batch

        records = generate_batch(batch_size=50, anomaly_rate=0.02)
        valid, _, _ = validate_batch(records)

        result = process_batch(spark=spark, records=valid)

        assert "metrics" in result
        assert "transformed_df" in result
        assert "settlements_df" in result
        assert "anomalies_df" in result
        assert result["metrics"]["records_read"] == len(valid)
