"""
audit_logger.py
Records every pipeline run to a local audit log (JSON Lines).
In Fabric this writes to a Delta table called pipeline_run_log.

This is your lineage and observability layer.
Every Bronze, Silver, and Gold job calls this before and after running.

Audit trail answers these questions:
  - When did this pipeline last run successfully?
  - How many records were processed?
  - How many records were rejected by DQ rules?
  - Which batch_id produced this data? (for tracing back to source)
  - How long did it take?
"""

import json
import uuid
from datetime import datetime
from pathlib import Path


class AuditLogger:
    """
    Logs pipeline run events to a local JSON Lines audit file.
    In production (Fabric), swap write_log() to write to a Delta table.

    Usage:
        logger   = AuditLogger()
        batch_id = logger.start_run("PL-001", "sales_orders_full")
        # ... run your pipeline ...
        logger.end_run(batch_id, status="success", records_read=200, records_written=198, records_rejected=2)
    """

    def __init__(self, log_dir: str = "logs"):
        self.log_dir  = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_dir / "pipeline_run_log.jsonl"
        self._active_runs: dict[str, dict] = {}

    def start_run(self, pipeline_id: str, pipeline_name: str, source_name: str = "") -> str:
        """
        Call this at the beginning of every pipeline run.
        Returns a batch_id that you attach to every record written in this run.
        The batch_id is how you trace any record back to its origin run.
        """
        batch_id = str(uuid.uuid4())
        run_record = {
            "batch_id":      batch_id,
            "pipeline_id":   pipeline_id,
            "pipeline_name": pipeline_name,
            "source_name":   source_name,
            "start_time":    datetime.utcnow().isoformat(),
            "end_time":      None,
            "duration_seconds": None,
            "status":        "running",
            "records_read":     0,
            "records_written":  0,
            "records_rejected": 0,
            "error_message":    None,
            "run_date":      str(datetime.utcnow().date()),
        }
        self._active_runs[batch_id] = run_record
        self._write_log(run_record)
        print(f"[AuditLogger] Run started | pipeline={pipeline_name} | batch_id={batch_id}")
        return batch_id

    def end_run(
        self,
        batch_id:         str,
        status:           str,
        records_read:     int = 0,
        records_written:  int = 0,
        records_rejected: int = 0,
        error_message:    str = None,
    ):
        """
        Call this at the end of every pipeline run.
        Status must be: success | partial | failed
        """
        if batch_id not in self._active_runs:
            print(f"[AuditLogger] Warning: batch_id {batch_id} not found in active runs")
            return

        run_record  = self._active_runs[batch_id]
        start_time  = datetime.fromisoformat(run_record["start_time"])
        end_time    = datetime.utcnow()
        duration    = round((end_time - start_time).total_seconds(), 2)

        run_record.update({
            "end_time":          end_time.isoformat(),
            "duration_seconds":  duration,
            "status":            status,
            "records_read":      records_read,
            "records_written":   records_written,
            "records_rejected":  records_rejected,
            "error_message":     error_message,
        })

        self._write_log(run_record)
        del self._active_runs[batch_id]

        emoji = "✓" if status == "success" else "⚠" if status == "partial" else "✗"
        print(
            f"[AuditLogger] {emoji} Run {status} | "
            f"pipeline={run_record['pipeline_name']} | "
            f"read={records_read} | written={records_written} | "
            f"rejected={records_rejected} | duration={duration}s"
        )

    def log_error(self, batch_id: str, error_message: str):
        """Log an error without ending the run — for mid-run error capture."""
        print(f"[AuditLogger] Error | batch_id={batch_id} | {error_message}")
        if batch_id in self._active_runs:
            self.end_run(batch_id, status="failed", error_message=error_message)

    def get_last_successful_run(self, pipeline_name: str) -> dict | None:
        """
        Returns the most recent successful run for a pipeline.
        Used by incremental pipelines to find their watermark:
        "only process records newer than my last successful run."
        """
        if not self.log_file.exists():
            return None

        matching_runs = []
        with open(self.log_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if (record.get("pipeline_name") == pipeline_name
                        and record.get("status") == "success"):
                    matching_runs.append(record)

        if not matching_runs:
            return None

        return max(matching_runs, key=lambda r: r["start_time"])

    def get_run_history(self, pipeline_name: str = None, limit: int = 20) -> list[dict]:
        """Return recent run history, optionally filtered by pipeline name."""
        if not self.log_file.exists():
            return []

        records = []
        with open(self.log_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if pipeline_name is None or record.get("pipeline_name") == pipeline_name:
                    if record.get("status") != "running":
                        records.append(record)

        records.sort(key=lambda r: r["start_time"], reverse=True)
        return records[:limit]

    def _write_log(self, record: dict):
        """Append a record to the audit log file."""
        with open(self.log_file, "a") as f:
            f.write(json.dumps(record) + "\n")
