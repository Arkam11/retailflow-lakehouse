"""
metadata_reader.py
The heart of the metadata-driven framework.

Reads pipeline configuration and DQ rules from JSON config files locally,
and from Delta tables when running inside Microsoft Fabric.

Every pipeline in this project calls MetadataReader first — it never
hardcodes source paths, table names, or load types anywhere else.

Key concept: configuration-driven design
  - Add a new data source? Insert a row in pipeline_config. Done.
  - Change a schedule? Update one field. Done.
  - Disable a pipeline? Set is_active=false. Done.
  - Zero code changes required for any of the above.
"""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional


class PipelineConfig:
    """
    Represents a single pipeline's configuration.
    Wraps the raw dict from the config table into a typed object
    so the rest of the codebase gets IDE autocomplete and clear field names.
    """

    def __init__(self, config_dict: dict):
        self.pipeline_id       = config_dict["pipeline_id"]
        self.pipeline_name     = config_dict["pipeline_name"]
        self.source_name       = config_dict["source_name"]
        self.source_type       = config_dict["source_type"]
        self.source_path       = config_dict["source_path"]
        self.file_format       = config_dict["file_format"]
        self.load_type         = config_dict["load_type"]
        self.watermark_column  = config_dict["watermark_column"]
        self.target_layer      = config_dict["target_layer"]
        self.target_table      = config_dict["target_table"]
        self.partition_columns = config_dict["partition_columns"]
        self.primary_key       = config_dict["primary_key"]
        self.dq_rules_id       = config_dict["dq_rules_id"]
        self.schedule          = config_dict["schedule"]
        self.is_active         = config_dict["is_active"]
        self.tags              = config_dict.get("tags", [])

    def __repr__(self):
        return (
            f"PipelineConfig("
            f"id={self.pipeline_id}, "
            f"name={self.pipeline_name}, "
            f"source={self.source_name}, "
            f"load_type={self.load_type}, "
            f"active={self.is_active})"
        )


class DQRule:
    """Represents a single data quality rule."""

    def __init__(self, rule_dict: dict):
        self.rule_id        = rule_dict["rule_id"]
        self.rule_name      = rule_dict["rule_name"]
        self.rule_type      = rule_dict["rule_type"]
        self.column         = rule_dict["column"]
        self.severity       = rule_dict["severity"]
        self.action_on_fail = rule_dict["action_on_fail"]
        # Optional fields depending on rule_type
        self.allowed_values = rule_dict.get("allowed_values", [])
        self.min_value      = rule_dict.get("min_value")
        self.max_value      = rule_dict.get("max_value")
        self.check          = rule_dict.get("check")

    def __repr__(self):
        return f"DQRule(id={self.rule_id}, name={self.rule_name}, severity={self.severity})"


class MetadataReader:
    """
    Central metadata reader for the RetailFlow lakehouse platform.

    In local/test mode: reads from JSON config files in config/
    In Fabric mode:     reads from Delta tables in OneLake

    Usage:
        reader   = MetadataReader()
        configs  = reader.get_active_pipelines()
        for config in configs:
            rules = reader.get_dq_rules(config.dq_rules_id)
            run_pipeline(config, rules)
    """

    def __init__(self, config_dir: str = "config", mode: str = "local"):
        """
        Args:
            config_dir: Path to config JSON files (local mode)
            mode:       "local" uses JSON files, "fabric" uses Delta tables
        """
        self.config_dir    = Path(config_dir)
        self.mode          = mode
        self._pipeline_cache  = None
        self._dq_rules_cache  = None
        print(f"MetadataReader initialized | mode={mode} | config_dir={config_dir}")

    def _load_pipeline_configs(self) -> list[dict]:
        """Load raw pipeline config dicts from JSON file."""
        if self._pipeline_cache is not None:
            return self._pipeline_cache

        config_file = self.config_dir / "pipeline_config.json"
        if not config_file.exists():
            raise FileNotFoundError(f"Pipeline config not found: {config_file}")

        with open(config_file) as f:
            data = json.load(f)

        self._pipeline_cache = data["pipelines"]
        print(f"Loaded {len(self._pipeline_cache)} pipeline configs from {config_file}")
        return self._pipeline_cache

    def _load_dq_rules(self) -> list[dict]:
        """Load raw DQ rule sets from JSON file."""
        if self._dq_rules_cache is not None:
            return self._dq_rules_cache

        dq_file = self.config_dir / "dq_rules.json"
        if not dq_file.exists():
            raise FileNotFoundError(f"DQ rules config not found: {dq_file}")

        with open(dq_file) as f:
            data = json.load(f)

        self._dq_rules_cache = data["dq_rule_sets"]
        print(f"Loaded {len(self._dq_rules_cache)} DQ rule sets from {dq_file}")
        return self._dq_rules_cache

    def get_all_pipelines(self) -> list[PipelineConfig]:
        """Return all pipelines regardless of active status."""
        return [PipelineConfig(p) for p in self._load_pipeline_configs()]

    def get_active_pipelines(self) -> list[PipelineConfig]:
        """
        Return only active pipelines.
        This is what the orchestrator calls — inactive pipelines are skipped.
        """
        configs = [PipelineConfig(p) for p in self._load_pipeline_configs()
                   if p["is_active"]]
        print(f"Active pipelines: {len(configs)}")
        return configs

    def get_pipeline_by_id(self, pipeline_id: str) -> Optional[PipelineConfig]:
        """Look up a specific pipeline by its ID."""
        for p in self._load_pipeline_configs():
            if p["pipeline_id"] == pipeline_id:
                return PipelineConfig(p)
        return None

    def get_pipeline_by_name(self, pipeline_name: str) -> Optional[PipelineConfig]:
        """Look up a specific pipeline by its name."""
        for p in self._load_pipeline_configs():
            if p["pipeline_name"] == pipeline_name:
                return PipelineConfig(p)
        return None

    def get_pipelines_by_source(self, source_name: str) -> list[PipelineConfig]:
        """Return all pipelines for a given source system."""
        return [PipelineConfig(p) for p in self._load_pipeline_configs()
                if p["source_name"] == source_name]

    def get_pipelines_by_tag(self, tag: str) -> list[PipelineConfig]:
        """Return all pipelines tagged with a specific label."""
        return [PipelineConfig(p) for p in self._load_pipeline_configs()
                if tag in p.get("tags", [])]

    def get_dq_rules(self, dq_rules_id: str) -> list[DQRule]:
        """
        Return all DQ rules for a given rule set ID.
        Called by the DQ engine before processing each table.
        """
        for rule_set in self._load_dq_rules():
            if rule_set["dq_rules_id"] == dq_rules_id:
                rules = [DQRule(r) for r in rule_set["rules"]]
                print(f"Loaded {len(rules)} DQ rules for {dq_rules_id}")
                return rules
        print(f"Warning: No DQ rules found for {dq_rules_id}")
        return []

    def get_critical_dq_rules(self, dq_rules_id: str) -> list[DQRule]:
        """Return only critical-severity rules for a rule set."""
        return [r for r in self.get_dq_rules(dq_rules_id)
                if r.severity == "critical"]

    def get_pipeline_summary(self) -> dict:
        """
        Returns a summary dict useful for monitoring dashboards.
        Shows counts by source type, load type, and schedule.
        """
        all_pipelines = self.get_all_pipelines()
        return {
            "total_pipelines":  len(all_pipelines),
            "active_pipelines": len([p for p in all_pipelines if p.is_active]),
            "by_source_type":   self._count_by(all_pipelines, "source_type"),
            "by_load_type":     self._count_by(all_pipelines, "load_type"),
            "by_schedule":      self._count_by(all_pipelines, "schedule"),
            "by_target_layer":  self._count_by(all_pipelines, "target_layer"),
        }

    @staticmethod
    def _count_by(pipelines: list, attr: str) -> dict:
        """Helper to count pipelines grouped by an attribute value."""
        counts = {}
        for p in pipelines:
            val = getattr(p, attr)
            counts[val] = counts.get(val, 0) + 1
        return counts
