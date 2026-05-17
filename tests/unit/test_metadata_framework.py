"""
test_metadata_framework.py
Tests for MetadataReader and AuditLogger.
"""

import json
import pytest
import tempfile
import time
from pathlib import Path
from src.ingestion.metadata_reader import MetadataReader, PipelineConfig, DQRule
from src.utils.audit_logger import AuditLogger


class TestMetadataReader:

    def setup_method(self):
        self.reader = MetadataReader(config_dir="config", mode="local")

    def test_loads_all_pipelines(self):
        pipelines = self.reader.get_all_pipelines()
        assert len(pipelines) == 7

    def test_all_pipelines_are_pipeline_config_objects(self):
        pipelines = self.reader.get_all_pipelines()
        for p in pipelines:
            assert isinstance(p, PipelineConfig)

    def test_active_pipelines_count(self):
        active = self.reader.get_active_pipelines()
        assert len(active) == 7

    def test_get_pipeline_by_id(self):
        p = self.reader.get_pipeline_by_id("PL-001")
        assert p is not None
        assert p.pipeline_name == "sales_orders_full"
        assert p.source_name   == "ecommerce_oms"
        assert p.load_type     == "incremental"

    def test_get_pipeline_by_id_not_found(self):
        p = self.reader.get_pipeline_by_id("PL-999")
        assert p is None

    def test_get_pipeline_by_name(self):
        p = self.reader.get_pipeline_by_name("inventory_snapshot")
        assert p is not None
        assert p.pipeline_id == "PL-003"
        assert p.source_type == "csv"

    def test_get_pipelines_by_source(self):
        crm = self.reader.get_pipelines_by_source("crm_database")
        assert len(crm) == 2
        names = [p.pipeline_name for p in crm]
        assert "customers_full_load" in names
        assert "customers_cdc"       in names

    def test_get_pipelines_by_tag_streaming(self):
        streaming = self.reader.get_pipelines_by_tag("streaming")
        assert len(streaming) == 1
        assert streaming[0].pipeline_name == "orders_realtime_stream"

    def test_get_pipelines_by_tag_sales(self):
        sales = self.reader.get_pipelines_by_tag("sales")
        assert len(sales) >= 2

    def test_pipeline_config_has_required_fields(self):
        p = self.reader.get_pipeline_by_id("PL-001")
        assert p.pipeline_id       == "PL-001"
        assert p.source_path       is not None
        assert p.target_table      is not None
        assert p.primary_key       is not None
        assert p.partition_columns is not None
        assert isinstance(p.partition_columns, list)
        assert isinstance(p.is_active, bool)
        assert isinstance(p.tags, list)

    def test_dq_rules_load_for_sales(self):
        rules = self.reader.get_dq_rules("DQ-001")
        assert len(rules) == 5
        for r in rules:
            assert isinstance(r, DQRule)

    def test_dq_rules_have_required_fields(self):
        rules = self.reader.get_dq_rules("DQ-001")
        for rule in rules:
            assert rule.rule_id        is not None
            assert rule.rule_name      is not None
            assert rule.rule_type      is not None
            assert rule.column         is not None
            assert rule.severity       in ["critical", "warning"]
            assert rule.action_on_fail in ["quarantine", "flag", "drop"]

    def test_critical_dq_rules_filter(self):
        all_rules      = self.reader.get_dq_rules("DQ-001")
        critical_rules = self.reader.get_critical_dq_rules("DQ-001")
        assert len(critical_rules) < len(all_rules)
        for rule in critical_rules:
            assert rule.severity == "critical"

    def test_dq_rules_unknown_id_returns_empty(self):
        rules = self.reader.get_dq_rules("DQ-999")
        assert rules == []

    def test_pipeline_summary_structure(self):
        summary = self.reader.get_pipeline_summary()
        assert "total_pipelines"  in summary
        assert "active_pipelines" in summary
        assert "by_source_type"   in summary
        assert "by_load_type"     in summary
        assert "by_schedule"      in summary

    def test_pipeline_summary_counts(self):
        summary = self.reader.get_pipeline_summary()
        assert summary["total_pipelines"]  == 7
        assert summary["active_pipelines"] == 7

    def test_pipeline_summary_source_types(self):
        summary    = self.reader.get_pipeline_summary()
        by_src     = summary["by_source_type"]
        assert by_src.get("rest_api")     == 2
        assert by_src.get("csv")          == 1
        assert by_src.get("sql")          == 2
        assert by_src.get("kafka")        == 1
        assert by_src.get("external_api") == 1

    def test_pipeline_summary_load_types(self):
        summary  = self.reader.get_pipeline_summary()
        by_load  = summary["by_load_type"]
        assert by_load.get("incremental") == 2
        assert by_load.get("full")        == 3
        assert by_load.get("cdc")         == 1
        assert by_load.get("streaming")   == 1

    def test_caching_works(self):
        # Second call should use cache (no re-read from disk)
        pipelines1 = self.reader.get_all_pipelines()
        pipelines2 = self.reader.get_all_pipelines()
        assert len(pipelines1) == len(pipelines2)


class TestAuditLogger:

    def setup_method(self):
        # Use a temp directory so tests don't pollute the real log
        self.tmp_dir = tempfile.mkdtemp()
        self.logger  = AuditLogger(log_dir=self.tmp_dir)

    def test_start_run_returns_batch_id(self):
        batch_id = self.logger.start_run("PL-001", "sales_orders_full", "ecommerce_oms")
        assert batch_id is not None
        assert len(batch_id) == 36  # UUID format

    def test_successful_run_logged(self):
        batch_id = self.logger.start_run("PL-001", "sales_orders_full", "ecommerce_oms")
        self.logger.end_run(batch_id, "success", records_read=100, records_written=98, records_rejected=2)
        history = self.logger.get_run_history("sales_orders_full")
        assert len(history) == 1
        assert history[0]["status"]           == "success"
        assert history[0]["records_read"]     == 100
        assert history[0]["records_written"]  == 98
        assert history[0]["records_rejected"] == 2

    def test_failed_run_logged(self):
        batch_id = self.logger.start_run("PL-003", "inventory_snapshot", "warehouse_wms")
        self.logger.log_error(batch_id, "File not found")
        history = self.logger.get_run_history("inventory_snapshot")
        assert len(history) == 1
        assert history[0]["status"]        == "failed"
        assert history[0]["error_message"] == "File not found"

    def test_duration_recorded(self):
        batch_id = self.logger.start_run("PL-001", "sales_orders_full", "ecommerce_oms")
        time.sleep(0.05)
        self.logger.end_run(batch_id, "success", records_read=10, records_written=10)
        history = self.logger.get_run_history("sales_orders_full")
        assert history[0]["duration_seconds"] >= 0.05

    def test_get_last_successful_run(self):
        batch_id = self.logger.start_run("PL-001", "sales_orders_full", "ecommerce_oms")
        self.logger.end_run(batch_id, "success", records_read=200, records_written=200)
        last = self.logger.get_last_successful_run("sales_orders_full")
        assert last is not None
        assert last["status"] == "success"

    def test_get_last_successful_run_ignores_failures(self):
        batch_id = self.logger.start_run("PL-001", "sales_orders_full", "ecommerce_oms")
        self.logger.log_error(batch_id, "Something broke")
        last = self.logger.get_last_successful_run("sales_orders_full")
        assert last is None

    def test_run_history_empty_for_unknown_pipeline(self):
        history = self.logger.get_run_history("nonexistent_pipeline")
        assert history == []

    def test_multiple_runs_history_ordered(self):
        for i in range(3):
            batch_id = self.logger.start_run("PL-001", "sales_orders_full", "ecommerce_oms")
            self.logger.end_run(batch_id, "success", records_read=i * 10, records_written=i * 10)
        history = self.logger.get_run_history("sales_orders_full")
        assert len(history) == 3
        # Most recent first
        assert history[0]["start_time"] >= history[1]["start_time"]

    def test_batch_id_in_log_record(self):
        batch_id = self.logger.start_run("PL-001", "sales_orders_full", "ecommerce_oms")
        self.logger.end_run(batch_id, "success", records_read=50, records_written=50)
        history = self.logger.get_run_history("sales_orders_full")
        assert history[0]["batch_id"] == batch_id
