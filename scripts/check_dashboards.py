"""Run every dashboard query and alert rule against the live Prometheus.

    python scripts/check_dashboards.py [--prometheus http://127.0.0.1:9090] [--grafana http://127.0.0.1:3000]

Reports, per panel query: OK (returns series), EMPTY (valid but no data yet) or
ERROR (Prometheus rejected it). Also checks scrape targets, rule health and,
when GRAFANA_ADMIN_PASSWORD is set (read from .env if present), that Grafana
provisioned every dashboard. Exit code 1 if any query errors, a rule is
unhealthy, a target is down, or a dashboard is missing.

EMPTY is not a failure: counters such as riskstream_rate_limited_total only
appear after the first event, and some panels only have data during a replay.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS = ROOT / "infrastructure" / "grafana" / "dashboards"


def get(url: str, headers: dict | None = None) -> dict:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def query(prom: str, expr: str) -> tuple[str, str]:
    expr = expr.replace("$__rate_interval", "2m")
    url = f"{prom}/api/v1/query?{urllib.parse.urlencode({'query': expr})}"
    try:
        body = get(url)
    except urllib.error.HTTPError as exc:
        return "ERROR", json.loads(exc.read()).get("error", str(exc))
    result = body["data"]["result"]
    return ("OK", f"{len(result)} series") if result else ("EMPTY", "")


def env_value(name: str) -> str | None:
    if os.environ.get(name):
        return os.environ[name]
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{name}="):
                return line.split("=", 1)[1].strip()
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prometheus", default="http://127.0.0.1:9090")
    ap.add_argument("--grafana", default="http://127.0.0.1:3000")
    ap.add_argument("--verbose", action="store_true", help="also list OK queries")
    args = ap.parse_args()
    failures = 0

    print("== scrape targets")
    for t in get(f"{args.prometheus}/api/v1/targets")["data"]["activeTargets"]:
        state = t["health"]
        print(f"  {state.upper():7} {t['labels']['job']:18} {t.get('lastError', '')}")
        failures += state != "up"

    print("== rules")
    groups = get(f"{args.prometheus}/api/v1/rules")["data"]["groups"]
    rules = [r for g in groups for r in g["rules"]]
    bad = [r for r in rules if r["health"] != "ok"]
    firing = [r["name"] for r in rules if r.get("state") == "firing"]
    print(f"  {len(rules)} rules loaded, {len(bad)} unhealthy, firing: {firing or 'none'}")
    for r in bad:
        print(f"  UNHEALTHY {r['name']}: {r.get('lastError')}")
    failures += len(bad)

    print("== dashboard queries")
    counts = {"OK": 0, "EMPTY": 0, "ERROR": 0}
    uids = []
    for f in sorted(DASHBOARDS.glob("*.json")):
        board = json.loads(f.read_text(encoding="utf-8"))
        uids.append(board["uid"])
        for p in board["panels"]:
            for t in p.get("targets", []):
                status, detail = query(args.prometheus, t["expr"])
                counts[status] += 1
                if status != "OK" or args.verbose:
                    print(f"  {status:5} {board['uid']} / {p['title']} [{t['refId']}] {detail}")
                    if status == "ERROR":
                        print(f"        {t['expr']}")
    print(f"  {counts}")
    failures += counts["ERROR"]

    password = env_value("GRAFANA_ADMIN_PASSWORD")
    if password:
        print("== grafana provisioning")
        auth = base64.b64encode(f"admin:{password}".encode()).decode()
        found = {d["uid"]: d for d in get(f"{args.grafana}/api/search?type=dash-db",
                                         {"Authorization": f"Basic {auth}"})}
        for uid in uids:
            ok = uid in found
            print(f"  {'OK' if ok else 'MISSING':7} {uid} {found.get(uid, {}).get('folderTitle', '')}")
            failures += not ok
        ds = get(f"{args.grafana}/api/datasources/uid/prometheus/health", {"Authorization": f"Basic {auth}"})
        print(f"  datasource prometheus: {ds.get('status')} {ds.get('message', '')}")
        failures += ds.get("status") != "OK"

    print("FAILED" if failures else "PASSED", f"({failures} problem(s))")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
