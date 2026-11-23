"""Generate the Grafana dashboards in infrastructure/grafana/dashboards/.

    python scripts/build_dashboards.py          # write the JSON files
    python scripts/build_dashboards.py --check  # exit 1 if the files are out of date

The JSON is generated (not hand-edited) so panel layout, datasource and units
stay consistent. tests/unit/test_observability_config.py checks that the
committed files match this script, and scripts/check_dashboards.py runs every
query against the live Prometheus.

Infrastructure and model behaviour are separate dashboards (spec section 12).
Data drift and labelled model performance are Phase 7.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "infrastructure" / "grafana" / "dashboards"
DS = {"type": "prometheus", "uid": "prometheus"}
APP = 'tier="app"'


class Board:
    def __init__(self, uid: str, title: str, description: str) -> None:
        self.uid, self.title, self.description = uid, title, description
        self.panels: list[dict] = []
        self.y = 0
        self.x = 0
        self.row_h = 0
        self.next_id = 1

    def _place(self, w: int, h: int) -> dict:
        if self.x + w > 24:
            self.x, self.y = 0, self.y + self.row_h
            self.row_h = 0
        pos = {"h": h, "w": w, "x": self.x, "y": self.y}
        self.x += w
        self.row_h = max(self.row_h, h)
        return pos

    def row(self, title: str) -> None:
        if self.x:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        self.panels.append({"id": self._id(), "type": "row", "title": title, "collapsed": False,
                            "gridPos": {"h": 1, "w": 24, "x": 0, "y": self.y}, "panels": []})
        self.y += 1

    def text(self, content: str, w: int = 24, h: int = 3) -> None:
        self.panels.append({"id": self._id(), "type": "text", "title": "", "gridPos": self._place(w, h),
                            "options": {"mode": "markdown", "content": content}})

    def _id(self) -> int:
        i = self.next_id
        self.next_id += 1
        return i

    def panel(self, kind: str, title: str, targets: list[tuple[str, str]], *, unit: str = "short", w: int = 12,
              h: int = 8, description: str = "", thresholds: list[tuple[float | None, str]] | None = None,
              options: dict | None = None, decimals: int | None = None, mappings: list | None = None,
              stack: bool = False, min_: float | None = None, max_: float | None = None) -> None:
        defaults: dict = {"unit": unit}
        if decimals is not None:
            defaults["decimals"] = decimals
        if min_ is not None:
            defaults["min"] = min_
        if max_ is not None:
            defaults["max"] = max_
        if thresholds:
            defaults["thresholds"] = {"mode": "absolute",
                                      "steps": [{"value": v, "color": c} for v, c in thresholds]}
        if mappings:
            defaults["mappings"] = mappings
        if kind == "timeseries":
            defaults["custom"] = {"lineWidth": 1, "fillOpacity": 10, "showPoints": "never",
                                  "stacking": {"mode": "normal" if stack else "none"}}
        p = {
            "id": self._id(), "type": kind, "title": title, "description": description, "datasource": DS,
            "gridPos": self._place(w, h),
            "targets": [{"refId": chr(65 + i), "expr": expr, "legendFormat": legend, "datasource": DS,
                         **({"instant": True, "format": "table"} if kind == "table" else {})}
                        for i, (expr, legend) in enumerate(targets)],
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": options or {},
        }
        if kind == "timeseries":
            p["options"] = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                            "tooltip": {"mode": "multi", "sort": "desc"}, **(options or {})}
        if kind == "stat":
            p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                            "colorMode": "background", "graphMode": "none", "textMode": "value", **(options or {})}
        self.panels.append(p)

    def stat(self, title, expr, unit="short", w=4, **kw):
        self.panel("stat", title, [(expr, "")], unit=unit, w=w, h=kw.pop("h", 4), **kw)

    def ts(self, title, targets, unit="short", w=12, **kw):
        self.panel("timeseries", title, targets, unit=unit, w=w, **kw)

    def json(self) -> dict:
        return {
            "uid": self.uid, "title": self.title, "description": self.description, "tags": ["riskstream"],
            "timezone": "browser", "schemaVersion": 39, "version": 1, "editable": True, "refresh": "30s",
            "time": {"from": "now-1h", "to": "now"}, "graphTooltip": 1,
            "templating": {"list": []}, "annotations": {"list": []},
            "links": [{"title": "RiskStream dashboards", "type": "dashboards", "tags": ["riskstream"],
                       "asDropdown": True}],
            "panels": self.panels,
        }


GOOD_BAD = [(None, "green"), (1, "red")]
BAD_GOOD = [(None, "red"), (1, "green")]
UPDOWN = [{"type": "value", "options": {"0": {"text": "DOWN", "color": "red"}, "1": {"text": "UP", "color": "green"}}}]
YESNO = [{"type": "value", "options": {"0": {"text": "NO", "color": "red"}, "1": {"text": "YES", "color": "green"}}}]


def q(quantile: float, metric: str, by: str = "", sel: str = "") -> str:
    grp = f"le{', ' + by if by else ''}"
    return f"histogram_quantile({quantile}, sum by ({grp}) (rate({metric}_bucket{{{sel}}}[$__rate_interval])))"


def api_board() -> Board:
    b = Board("riskstream-api", "RiskStream / API & Infrastructure",
              "Request traffic, latency, errors, protection, process resources and dependencies.")
    b.row("Overview")
    b.stat("API instances up", 'count(up{job="api"} == 1) or vector(0)', thresholds=BAD_GOOD)
    b.stat("Requests / s (/v1)", 'sum(rate(riskstream_http_requests_total{route=~"/v1/.*"}[$__rate_interval])) or vector(0)',
           unit="reqps", decimals=1, thresholds=[(None, "blue")])
    b.stat("5xx ratio (/v1, 5m)", "riskstream:http_5xx_ratio:rate5m or vector(0)", unit="percentunit", decimals=2,
           thresholds=[(None, "green"), (0.01, "orange"), (0.05, "red")])
    b.stat("/v1/score p95", "riskstream:score_latency_seconds:p95_5m", unit="s", decimals=3,
           thresholds=[(None, "green"), (0.1, "orange"), (0.25, "red")])
    b.stat("Model loaded", "min(riskstream_model_loaded)", mappings=YESNO, thresholds=BAD_GOOD)
    b.stat("Firing alerts", 'count(ALERTS{alertstate="firing"}) or vector(0)', thresholds=GOOD_BAD)

    b.row("Traffic")
    b.ts("Requests / s by route", [("sum by (route) (rate(riskstream_http_requests_total[$__rate_interval]))",
                                    "{{route}}")], unit="reqps")
    b.ts("/v1 responses / s by status", [('sum by (status) (rate(riskstream_http_requests_total{route=~"/v1/.*"}[$__rate_interval]))',
                                          "{{status}}")], unit="reqps", stack=True)
    b.ts("4xx and 5xx / s (/v1)", [
        ('sum(rate(riskstream_http_requests_total{route=~"/v1/.*", status=~"4.."}[$__rate_interval])) or vector(0)', "4xx"),
        ('sum(rate(riskstream_http_requests_total{route=~"/v1/.*", status=~"5.."}[$__rate_interval])) or vector(0)', "5xx"),
    ], unit="reqps")
    b.ts("Error ratio (/v1)", [("riskstream:http_5xx_ratio:rate5m", "5xx share")], unit="percentunit")

    b.row("Latency")
    b.ts("/v1/score latency (server side)", [
        (q(0.50, "riskstream_http_request_duration_seconds", sel='route="/v1/score"'), "p50"),
        (q(0.95, "riskstream_http_request_duration_seconds", sel='route="/v1/score"'), "p95"),
        (q(0.99, "riskstream_http_request_duration_seconds", sel='route="/v1/score"'), "p99"),
    ], unit="s")
    b.ts("p95 latency by route", [(q(0.95, "riskstream_http_request_duration_seconds", by="route"), "{{route}}")], unit="s")
    b.ts("Scoring stages p95", [(q(0.95, "riskstream_score_stage_seconds", by="stage"), "{{stage}}")], unit="s",
         description="idempotency check, history query (Redis or PostgreSQL), feature derivation, model, persist")
    b.ts("Feature read p95 (Redis, from the API)", [(q(0.95, "riskstream_feature_store_read_seconds"), "p95"),
                                                    (q(0.50, "riskstream_feature_store_read_seconds"), "p50")], unit="s")

    b.row("Protection")
    b.ts("In-flight /v1 requests", [("sum(riskstream_http_inflight_requests)", "in flight")])
    b.ts("Load shed, rate limited, auth failures / s", [
        ("sum(rate(riskstream_load_shed_total[$__rate_interval])) or vector(0)", "load shed (503)"),
        ("sum(rate(riskstream_rate_limited_total[$__rate_interval])) or vector(0)", "rate limited (429)"),
        ("sum by (reason) (rate(riskstream_auth_failures_total[$__rate_interval]))", "auth: {{reason}}"),
    ], unit="reqps")

    b.row("Process resources (app containers)")
    b.ts("CPU (cores) by service", [(f"rate(process_cpu_seconds_total{{{APP}}}[$__rate_interval])", "{{job}}")], unit="none",
         w=8)
    b.ts("Resident memory by service", [(f"process_resident_memory_bytes{{{APP}}}", "{{job}}")], unit="bytes", w=8)
    b.ts("Open file descriptors", [(f"process_open_fds{{{APP}}}", "{{job}}")], w=8)

    b.row("Dependencies")
    b.stat("PostgreSQL", "max(pg_up)", mappings=UPDOWN, thresholds=BAD_GOOD, w=3)
    b.stat("Redis", "max(redis_up)", mappings=UPDOWN, thresholds=BAD_GOOD, w=3)
    b.stat("Redpanda brokers", "max(redpanda_cluster_brokers)", thresholds=BAD_GOOD, w=3)
    b.stat("Online features usable", "min(riskstream_online_store_usable)", mappings=YESNO, thresholds=BAD_GOOD, w=3)
    b.stat("PG connections used", 'sum(pg_stat_activity_count) / max(pg_settings_max_connections)', unit="percentunit",
           thresholds=[(None, "green"), (0.6, "orange"), (0.8, "red")], w=4)
    b.stat("API pool in use", "sum(riskstream_db_pool_checked_out) / sum(riskstream_db_pool_capacity)",
           unit="percentunit", thresholds=[(None, "green"), (0.7, "orange"), (0.9, "red")], w=4)
    b.stat("Redis memory used", "max(redis_memory_used_bytes) / max(redis_memory_max_bytes)", unit="percentunit",
           thresholds=[(None, "green"), (0.7, "orange"), (0.85, "red")], w=4)
    b.ts("PostgreSQL connections by state", [
        ('sum by (state) (pg_stat_activity_count{datname="riskstream"})', "{{state}}"),
        ("max(pg_settings_max_connections)", "max_connections"),
    ], w=8)
    b.ts("PostgreSQL transactions / s", [
        ('sum(rate(pg_stat_database_xact_commit{datname="riskstream"}[$__rate_interval]))', "commits"),
        ('sum(rate(pg_stat_database_xact_rollback{datname="riskstream"}[$__rate_interval]))', "rollbacks"),
    ], unit="ops", w=8)
    b.ts("API connection pool", [("sum(riskstream_db_pool_checked_out)", "checked out"),
                                 ("sum(riskstream_db_pool_capacity)", "capacity")], w=8)
    b.ts("Redis memory", [("max(redis_memory_used_bytes)", "used"), ("max(redis_memory_max_bytes)", "maxmemory")],
         unit="bytes", w=8)
    b.ts("Redis commands / s", [("sum(rate(redis_commands_processed_total[$__rate_interval]))", "commands")], unit="ops", w=8)
    b.ts("Dependency errors / s", [("sum by (dependency) (rate(riskstream_dependency_errors_total[$__rate_interval]))",
                                    "{{dependency}}")], unit="ops", w=8)

    b.row("Alerts")
    b.panel("table", "Firing and pending alerts", [('ALERTS', "")], w=24, h=7,
            options={"showHeader": True, "cellHeight": "sm"},
            description="All alert rules: infrastructure/prometheus/alerts.yml. Runbooks: docs/incident_runbooks.md")
    return b


def streaming_board() -> Board:
    b = Board("riskstream-streaming", "RiskStream / Streaming & Online Features",
              "Outbox relay, Redpanda, consumers, dead letters and the Redis online feature store.")
    b.row("Overview")
    b.stat("Online features usable", "min(riskstream_online_store_usable)", mappings=YESNO, thresholds=BAD_GOOD)
    b.stat("Outbox backlog", "max(riskstream_outbox_backlog_events)", thresholds=[(None, "green"), (1000, "orange")])
    b.stat("Oldest unpublished", "max(riskstream_outbox_oldest_unpublished_age_seconds)", unit="s",
           thresholds=[(None, "green"), (5, "orange"), (60, "red")])
    b.stat("Feature consumer lag (broker)", "max(riskstream_feature_consumer_broker_lag_messages)",
           thresholds=[(None, "green"), (100, "orange"), (1000, "red")])
    b.stat("Scored -> Redis p95", q(0.95, "riskstream_stream_event_age_seconds"), unit="s", decimals=2,
           thresholds=[(None, "green"), (2, "orange"), (5, "red")])
    b.stat("Dead letters (1h)", "sum(increase(riskstream_dlq_messages_total[1h])) or vector(0)", thresholds=GOOD_BAD)

    b.row("Outbox relay")
    b.ts("Published / s by topic", [("sum by (topic) (rate(riskstream_outbox_published_total[$__rate_interval]))",
                                     "{{topic}}"),
                                    ("sum by (topic) (rate(riskstream_outbox_publish_failures_total[$__rate_interval]))",
                                     "failed {{topic}}")], unit="ops")
    b.ts("Backlog and oldest unpublished age", [("max(riskstream_outbox_backlog_events)", "backlog (events)"),
                                                ("max(riskstream_outbox_oldest_unpublished_age_seconds)", "oldest (s)")])
    b.ts("Relay batch time p95", [(q(0.95, "riskstream_outbox_batch_seconds"), "p95")], unit="s")

    b.row("Consumers")
    b.ts("Consumer lag by consumer", [("sum by (consumer) (riskstream_consumer_lag_messages)", "{{consumer}}"),
                                      ("max(riskstream_feature_consumer_broker_lag_messages)", "feature-updater (broker)")])
    b.ts("Messages / s by outcome", [("sum by (consumer, outcome) (rate(riskstream_stream_messages_total[$__rate_interval]))",
                                      "{{consumer}} {{outcome}}")], unit="ops")
    b.ts("Event age when applied (scored -> Redis)", [(q(0.50, "riskstream_stream_event_age_seconds"), "p50"),
                                                      (q(0.95, "riskstream_stream_event_age_seconds"), "p95")], unit="s")
    b.ts("Processing time p95", [(q(0.95, "riskstream_stream_process_seconds", by="consumer"), "{{consumer}}")], unit="s")
    b.ts("Blocked / stalled", [("max by (consumer) (riskstream_consumer_blocked)", "blocked {{consumer}}"),
                               ("max(riskstream_feature_consumer_stalled_seconds)", "feature consumer stalled (s)")])
    b.ts("Dead letters / s", [("sum by (consumer, error_class) (rate(riskstream_dlq_messages_total[$__rate_interval]))",
                               "{{consumer}} {{error_class}}")], unit="ops")

    b.row("Online feature store")
    b.ts("Feature source / s", [("sum by (source) (rate(riskstream_feature_source_total[$__rate_interval]))",
                                 "{{source}}")], unit="ops", stack=True)
    b.ts("Fallbacks / s by reason", [("sum by (reason) (rate(riskstream_feature_fallbacks_total[$__rate_interval]))",
                                      "{{reason}}")], unit="ops")
    b.panel("table", "Current freshness verdict", [("max by (reason) (riskstream_online_store_state) == 1", "")],
            w=8, h=6, description="What the API decides right now; 'ok' means Redis features are used.")
    b.ts("Redis memory and keys", [("max(redis_memory_used_bytes)", "used bytes"),
                                   ("sum(redis_db_keys)", "keys")], w=8, h=6)
    b.ts("Replay published / s", [("sum(rate(riskstream_replay_published_total[$__rate_interval])) or vector(0)",
                                   "replay")], unit="ops", w=8, h=6,
         description="Only non-zero while a replay runs in a scraped process (host replays are not scraped).")

    b.row("Redpanda")
    b.ts("Records produced / fetched / s", [("sum(rate(redpanda_kafka_records_produced_total[$__rate_interval]))", "produced"),
                                            ("sum(rate(redpanda_kafka_records_fetched_total[$__rate_interval]))", "fetched")],
         unit="ops", w=8)
    b.ts("Kafka request latency p95", [(q(0.95, "redpanda_kafka_request_latency_seconds", by="redpanda_request"),
                                        "{{redpanda_request}}")], unit="s", w=8)
    b.ts("Partitions / unavailable / disk free", [("max(redpanda_cluster_partitions)", "partitions"),
                                                  ("max(redpanda_cluster_unavailable_partitions)", "unavailable"),
                                                  ("max(redpanda_storage_disk_free_bytes) / 1e9", "disk free (GB)")], w=8)
    return b


def model_board() -> Board:
    b = Board("riskstream-model-serving", "RiskStream / Model Serving",
              "Decisions and score distribution of the served model. Drift and labelled performance: Phase 7.")
    b.text("**Simulated decision policy on PaySim data.** This board shows what the model decides, not whether it "
           "is right: delayed-label performance and data drift arrive in Phase 7.", h=2)
    b.row("Overview")
    b.panel("table", "Loaded model and policy", [("max by (model_version, policy_version, feature_version) "
                                                  "(riskstream_model_info)", "")], w=12, h=4)
    b.stat("Decisions / s", "sum(rate(riskstream_decisions_total[$__rate_interval])) or vector(0)", unit="ops",
           decimals=2, thresholds=[(None, "blue")])
    b.stat("Decline share", 'sum(rate(riskstream_decisions_total{decision="DECLINE"}[$__rate_interval])) '
           "/ clamp_min(sum(rate(riskstream_decisions_total[$__rate_interval])), 1e-9)", unit="percentunit", decimals=2,
           thresholds=[(None, "blue")])
    b.stat("Review share", 'sum(rate(riskstream_decisions_total{decision="REVIEW"}[$__rate_interval])) '
           "/ clamp_min(sum(rate(riskstream_decisions_total[$__rate_interval])), 1e-9)", unit="percentunit", decimals=2,
           thresholds=[(None, "blue")])

    b.row("Decisions")
    b.ts("Decisions / s by outcome", [("sum by (decision) (rate(riskstream_decisions_total[$__rate_interval]))",
                                       "{{decision}}")], unit="ops", stack=True)
    b.ts("Decisions / s by model version", [("sum by (model_version) (rate(riskstream_decisions_total[$__rate_interval]))",
                                             "{{model_version}}")], unit="ops")
    b.panel("heatmap", "Fraud probability distribution",
            [("sum by (le) (increase(riskstream_fraud_probability_bucket[$__rate_interval]))", "{{le}}")],
            unit="short", w=12, description="Calibrated probabilities of scored transactions (histogram buckets).",
            options={"calculate": False, "yAxis": {"axisPlacement": "left"}, "cellGap": 1,
                     "color": {"mode": "scheme", "scheme": "Oranges", "steps": 64},
                     "rowsFrame": {"layout": "auto"}, "tooltip": {"show": True}})
    b.ts("Mean fraud probability", [("sum(rate(riskstream_fraud_probability_sum[$__rate_interval])) "
                                     "/ clamp_min(sum(rate(riskstream_fraud_probability_count[$__rate_interval])), 1e-9)",
                                     "mean p")], unit="percentunit")

    b.row("Requests and labels")
    b.ts("Idempotent replays and conflicts / s", [
        ("sum by (endpoint) (rate(riskstream_idempotent_replays_total[$__rate_interval]))", "replay {{endpoint}}"),
        ("sum by (endpoint) (rate(riskstream_conflicts_total[$__rate_interval]))", "conflict {{endpoint}}"),
    ], unit="ops")
    b.ts("Labels recorded / s", [("sum by (source, is_fraud) (rate(riskstream_labels_total[$__rate_interval]))",
                                  "{{source}} fraud={{is_fraud}}")], unit="ops")
    b.ts("Model inference p95", [(q(0.95, "riskstream_score_stage_seconds", sel='stage="model_inference"'), "p95"),
                                 (q(0.50, "riskstream_score_stage_seconds", sel='stage="model_inference"'), "p50")],
         unit="s")
    b.ts("Feature source / s", [("sum by (source) (rate(riskstream_feature_source_total[$__rate_interval]))",
                                 "{{source}}")], unit="ops")
    return b


BOARDS = (api_board, streaming_board, model_board)


def render() -> dict[str, str]:
    boards = [build() for build in BOARDS]
    return {f"{b.uid}.json": json.dumps(b.json(), indent=2, sort_keys=False) + "\n" for b in boards}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    files = render()
    if args.check:
        stale = [n for n, c in files.items() if not (OUT / n).exists() or (OUT / n).read_text(encoding="utf-8") != c]
        if stale:
            print("out of date:", stale, "-> run python scripts/build_dashboards.py")
            return 1
        print("dashboards up to date")
        return 0
    OUT.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (OUT / name).write_text(content, encoding="utf-8")
        print("wrote", OUT / name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
