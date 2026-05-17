"""
setup_metadata_tables.py
Demonstrates and validates the complete metadata framework.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.metadata_reader import MetadataReader
from src.utils.audit_logger import AuditLogger


def print_section(title):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def demonstrate_metadata_framework():
    print_section("RetailFlow Metadata Framework — Setup & Validation")

    # ── 1. Load pipeline configurations ─────────────────────────
    print_section("1. Loading Pipeline Configurations")
    reader       = MetadataReader(config_dir="config", mode="local")
    all_pipelines = reader.get_all_pipelines()

    print(f"\nAll pipelines ({len(all_pipelines)} total):")
    for p in all_pipelines:
        status = "ACTIVE" if p.is_active else "INACTIVE"
        print(f"  [{status}] {p.pipeline_id} | {p.pipeline_name:<30} | {p.load_type:<12} | {p.schedule}")

    # ── 2. Active pipelines ──────────────────────────────────────
    print_section("2. Active Pipelines (what orchestrator will run)")
    active = reader.get_active_pipelines()
    print(f"  {len(active)} of {len(all_pipelines)} pipelines are active")

    # ── 3. Filter by source ─────────────────────────────────────
    print_section("3. Filter by Source System")
    crm_pipelines = reader.get_pipelines_by_source("crm_database")
    print(f"  CRM database pipelines: {len(crm_pipelines)}")
    for p in crm_pipelines:
        print(f"    - {p.pipeline_name} | load_type={p.load_type}")

    # ── 4. Filter by tag ────────────────────────────────────────
    print_section("4. Filter by Tag")
    streaming_pipelines = reader.get_pipelines_by_tag("streaming")
    print(f"  Streaming pipelines: {len(streaming_pipelines)}")
    for p in streaming_pipelines:
        print(f"    - {p.pipeline_name} | source={p.source_name}")

    # ── 5. DQ rules ─────────────────────────────────────────────
    print_section("5. Data Quality Rules")
    sales_pipeline = reader.get_pipeline_by_id("PL-001")
    dq_rules       = reader.get_dq_rules(sales_pipeline.dq_rules_id)
    print(f"  DQ rules for {sales_pipeline.pipeline_name}:")
    for rule in dq_rules:
        print(f"    [{rule.severity.upper():<8}] {rule.rule_name} | column={rule.column} | on_fail={rule.action_on_fail}")

    critical_rules = reader.get_critical_dq_rules(sales_pipeline.dq_rules_id)
    print(f"\n  Critical rules: {len(critical_rules)} of {len(dq_rules)} total")

    # ── 6. Platform summary ─────────────────────────────────────
    print_section("6. Platform Summary")
    summary = reader.get_pipeline_summary()
    total    = summary["total_pipelines"]
    active_n = summary["active_pipelines"]
    by_src   = summary["by_source_type"]
    by_load  = summary["by_load_type"]
    by_sched = summary["by_schedule"]
    print(f"  Total pipelines:  {total}")
    print(f"  Active pipelines: {active_n}")
    print(f"  By source type:   {by_src}")
    print(f"  By load type:     {by_load}")
    print(f"  By schedule:      {by_sched}")

    # ── 7. Audit logger simulation ──────────────────────────────
    print_section("7. Audit Logger — Simulating Pipeline Runs")
    logger = AuditLogger(log_dir="logs")

    # Simulate a successful run
    batch_id = logger.start_run(
        pipeline_id   = "PL-001",
        pipeline_name = "sales_orders_full",
        source_name   = "ecommerce_oms",
    )
    time.sleep(0.1)
    logger.end_run(
        batch_id         = batch_id,
        status           = "success",
        records_read     = 200,
        records_written  = 198,
        records_rejected = 2,
    )

    # Simulate a failed run
    batch_id_2 = logger.start_run(
        pipeline_id   = "PL-003",
        pipeline_name = "inventory_snapshot",
        source_name   = "warehouse_wms",
    )
    logger.log_error(batch_id_2, "Source file not found: inventory_snapshot_*.csv")

    # Show run history
    print("\n  Run history:")
    history = logger.get_run_history(limit=5)
    for run in history:
        name     = run["pipeline_name"]
        status   = run["status"].upper()
        read     = run["records_read"]
        written  = run["records_written"]
        duration = run["duration_seconds"]
        print(f"    [{status:<8}] {name:<30} | read={read} | written={written} | duration={duration}s")

    # Show watermark
    last_run = logger.get_last_successful_run("sales_orders_full")
    if last_run:
        ts = last_run["start_time"]
        print(f"\n  Last successful run of sales_orders_full: {ts}")
        print(f"  Incremental pipeline will use this as its watermark timestamp")

    print_section("Setup Complete — Metadata Framework Validated")
    print("  All systems ready for Phase 3 (Bronze Layer)\n")


if __name__ == "__main__":
    demonstrate_metadata_framework()
