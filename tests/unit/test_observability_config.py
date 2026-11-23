"""Phase 6 observability config: dashboards, alert rules, runbooks, metrics and log fields.

These run without Docker. The promtool rule tests run only when Docker is
available (skipped otherwise); the same command is in docs/monitoring.md.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from prometheus_client import REGISTRY

import riskstream.monitoring.metrics  # noqa: F401  (registers collectors)
import riskstream.monitoring.stream_metrics  # noqa: F401
from riskstream.logging_config import JSONFormatter

ROOT = Path(__file__).resolve().parents[2]
PROM = ROOT / "infrastructure" / "prometheus"
DASHBOARDS = ROOT / "infrastructure" / "grafana" / "dashboards"
RUNBOOKS = ROOT / "docs" / "incident_runbooks.md"

# Metrics from exporters / Prometheus itself that our dashboards and alerts may use.
EXTERNAL_PREFIXES = ("pg_", "redis_", "redpanda_", "process_")
EXTERNAL_NAMES = {"up", "ALERTS", "vector"}
PROMQL_WORDS = {
    "sum", "max", "min", "count", "avg", "by", "without", "rate", "increase", "histogram_quantile", "clamp_min",
    "or", "and", "unless", "on", "ignoring", "le", "vector", "bool", "offset", "irate", "abs",
}


def load_rules() -> list[dict]:
    doc = yaml.safe_load((PROM / "alerts.yml").read_text(encoding="utf-8"))
    return [r for g in doc["groups"] for r in g["rules"]]


def dashboard_exprs() -> list[tuple[str, str, str]]:
    out = []
    for f in sorted(DASHBOARDS.glob("*.json")):
        for p in json.loads(f.read_text(encoding="utf-8"))["panels"]:
            for t in p.get("targets", []):
                out.append((f.name, p["title"], t["expr"]))
    return out


def metric_names(expr: str) -> set[str]:
    """Identifiers used as metric names in a PromQL expression (labels and functions removed)."""
    stripped = re.sub(r"\{[^}]*\}", "", expr)          # label matchers
    stripped = re.sub(r'"[^"]*"', "", stripped)         # string literals
    stripped = re.sub(r"\[[^\]]*\]", "", stripped)      # range selectors
    stripped = re.sub(r"\bby\s*\([^)]*\)", "", stripped)
    stripped = re.sub(r"\bwithout\s*\([^)]*\)", "", stripped)
    stripped = re.sub(r"(?<![A-Za-z0-9_:])\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", "", stripped)  # numbers, 1e-9
    names = set(re.findall(r"[A-Za-z_:][A-Za-z0-9_:]*", stripped))
    return {n for n in names if n not in PROMQL_WORDS}


def known_metric(name: str, recorded: set[str]) -> bool:
    if name in recorded or name in EXTERNAL_NAMES or name.startswith(EXTERNAL_PREFIXES):
        return True
    return name.startswith("riskstream_") and name in REGISTRY._names_to_collectors


def anchor(heading: str) -> str:
    """GitHub-style heading anchor."""
    a = heading.strip().lower()
    a = re.sub(r"[^a-z0-9 _-]", "", a)
    return a.replace(" ", "-")


# --- dashboards ---------------------------------------------------------------

def test_dashboards_match_generator():
    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "build_dashboards.py"), "--check"],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stdout + r.stderr


def test_dashboards_have_unique_uids_ids_and_prometheus_datasource():
    uids = set()
    for f in DASHBOARDS.glob("*.json"):
        board = json.loads(f.read_text(encoding="utf-8"))
        assert board["uid"] not in uids
        uids.add(board["uid"])
        ids = [p["id"] for p in board["panels"]]
        assert len(ids) == len(set(ids)), f"duplicate panel ids in {f.name}"
        for p in board["panels"]:
            if p["type"] not in ("row", "text"):
                assert p["datasource"]["uid"] == "prometheus", (f.name, p["title"])
                assert p["targets"], (f.name, p["title"])
            assert p["gridPos"]["x"] + p["gridPos"]["w"] <= 24, (f.name, p["title"])
    assert len(uids) == 3


def test_dashboard_queries_reference_known_metrics():
    recorded = {r["record"] for r in load_rules() if "record" in r}
    unknown = [(f, title, n) for f, title, expr in dashboard_exprs()
               for n in metric_names(expr) if not known_metric(n, recorded)]
    assert not unknown, unknown


def test_dashboard_queries_have_balanced_brackets():
    for f, title, expr in dashboard_exprs():
        for o, c in ("()", "{}", "[]"):
            assert expr.count(o) == expr.count(c), (f, title, expr)


def test_metric_name_extraction():
    names = metric_names('sum by (le, route) (rate(riskstream_x_bucket{route="/v1/score"}[5m])) '
                         '/ clamp_min(max(up{tier="app"}), 1e-9) > 650e6')
    assert names == {"riskstream_x_bucket", "up"}


# --- alert rules -------------------------------------------------------------

def test_every_alert_has_severity_summary_and_runbook():
    alerts = [r for r in load_rules() if "alert" in r]
    assert len(alerts) >= 20
    names = [a["alert"] for a in alerts]
    assert len(names) == len(set(names))
    for a in alerts:
        assert a["labels"]["severity"] in ("critical", "warning"), a["alert"]
        assert a["annotations"]["summary"] and a["annotations"]["description"], a["alert"]
        assert a["annotations"]["runbook_url"] == f"docs/incident_runbooks.md#{a['alert'].lower()}", a["alert"]


def test_every_runbook_anchor_exists():
    headings = {anchor(m) for m in re.findall(r"^## (.+)$", RUNBOOKS.read_text(encoding="utf-8"), re.M)}
    missing = [a["alert"] for a in load_rules() if "alert" in a and a["alert"].lower() not in headings]
    assert not missing, missing


def test_alert_expressions_reference_known_metrics():
    rules = load_rules()
    recorded = {r["record"] for r in rules if "record" in r}
    unknown = [(r.get("alert") or r.get("record"), n) for r in rules for n in metric_names(r["expr"])
               if not known_metric(n, recorded)]
    assert not unknown, unknown


def test_every_alert_has_a_promtool_unit_test():
    tests = yaml.safe_load((PROM / "tests" / "alerts_test.yml").read_text(encoding="utf-8"))
    tested = {c["alertname"] for t in tests["tests"] for c in t.get("alert_rule_test", [])}
    alerts = {r["alert"] for r in load_rules() if "alert" in r}
    assert alerts <= tested, sorted(alerts - tested)


def test_prometheus_config_loads_rules_and_scrapes_exporters():
    cfg = yaml.safe_load((PROM / "prometheus.yml").read_text(encoding="utf-8"))
    assert "/etc/prometheus/alerts.yml" in cfg["rule_files"]
    jobs = {j["job_name"]: j for j in cfg["scrape_configs"]}
    for job in ("api", "redis", "postgres"):
        assert job in jobs
    for j in jobs.values():
        tiers = {sc.get("labels", {}).get("tier") for sc in j["static_configs"]}
        assert tiers <= {"app", "infra"} and None not in tiers, j["job_name"]


def test_login_roles_can_connect_to_riskstream_database():
    """roles.sql revokes CONNECT from PUBLIC, so every non-owner login role needs an explicit
    grant. (The monitor role once lacked it: postgres_exporter reported pg_up=0.)"""
    sql = (ROOT / "infrastructure" / "postgres" / "roles.sql").read_text(encoding="utf-8")
    assert "REVOKE ALL ON DATABASE riskstream FROM PUBLIC" in sql
    roles = set(re.findall(r"CREATE ROLE (\w+) LOGIN", sql)) - {"riskstream_owner"}
    assert roles == {"riskstream_app", "riskstream_monitor"}
    for role in roles:
        assert f"GRANT CONNECT ON DATABASE riskstream TO {role};" in sql, role


def test_exporter_connect_timeouts_below_scrape_timeout():
    """A hung dependency must yield redis_up/pg_up = 0, not a timed-out scrape."""
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    scrape_timeout = float(yaml.safe_load((PROM / "prometheus.yml").read_text(encoding="utf-8"))
                           ["global"]["scrape_timeout"].rstrip("s"))
    redis_env = compose["services"]["redis-exporter"]["environment"]
    assert float(redis_env["REDIS_EXPORTER_CONNECTION_TIMEOUT"].rstrip("s")) < scrape_timeout
    uri = compose["services"]["postgres-exporter"]["environment"]["DATA_SOURCE_URI"]
    assert float(re.search(r"connect_timeout=(\d+)", uri).group(1)) < scrape_timeout


def _docker_ok() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.mark.skipif(not _docker_ok(), reason="Docker not available")
def test_promtool_rule_tests_pass():
    r = subprocess.run(["docker", "run", "--rm", "-v", f"{PROM}:/p", "--entrypoint", "promtool",
                        "prom/prometheus:v3.5.0", "test", "rules", "/p/tests/alerts_test.yml"],
                       capture_output=True, text=True, timeout=300)
    assert r.returncode == 0 and "SUCCESS" in r.stdout + r.stderr, r.stdout + r.stderr


# --- logs --------------------------------------------------------------------

def test_json_log_has_service_instance_environment():
    fmt = JSONFormatter(service="api", instance="api-1", environment="local")
    rec = logging.LogRecord("riskstream.x", logging.INFO, __file__, 1, "hello %s", ("world",), None)
    rec.request_id = "r1"
    out = json.loads(fmt.format(rec))
    assert out["service"] == "api" and out["instance"] == "api-1" and out["environment"] == "local"
    assert out["message"] == "hello world" and out["request_id"] == "r1" and out["level"] == "INFO"


def test_json_log_omits_unset_static_fields():
    out = json.loads(JSONFormatter().format(logging.LogRecord("x", logging.INFO, __file__, 1, "m", (), None)))
    assert "service" not in out and "instance" not in out
