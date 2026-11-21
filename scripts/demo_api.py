"""Live HTTP walkthrough against a running RiskStream API.

    python -m riskstream.cli serve            # in another terminal
    python scripts/demo_api.py [--base-url http://127.0.0.1:8000] [--key-file secrets/demo_api_key.txt]

Uses PaySim test-period rows (step >= 670; not seeded, not used by the parity
check) so each transaction has real stored history and has not been scored yet.
Prints status codes and responses only. Nothing here is a performance benchmark.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
import uuid
from datetime import timedelta
from pathlib import Path

import httpx
import polars as pl

from riskstream.config import PROJECT_ROOT, AppConfig
from riskstream.data.ingestion import load_interim


def show(title: str, r: httpx.Response, keys: list[str] | None = None) -> dict:
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    shown = {k: body.get(k) for k in keys} if keys and "error" not in body else body
    print(f"\n== {title}\n   HTTP {r.status_code}  request_id={r.headers.get('x-request-id')}")
    print("   " + json.dumps(shown, default=str)[:600])
    return body


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    ap.add_argument("--key-file", default=str(PROJECT_ROOT / "secrets" / "demo_api_key.txt"))
    ap.add_argument("--latency-sample", type=int, default=200)
    args = ap.parse_args()
    key = Path(args.key_file).read_text().strip()
    h = {"X-API-Key": key}
    epoch = AppConfig.load().simulation_epoch
    run = uuid.uuid4().hex[:6]

    rows = (load_interim("paysim").filter(pl.col("step") >= 670).sort("step", "event_seq").collect())
    fraud = rows.filter(pl.col("is_fraud") == 1).head(3)
    legit = rows.filter(pl.col("is_fraud") == 0).head(args.latency_sample)

    def body(r: dict, suffix: str = "") -> dict:
        return {"transaction_id": r["transaction_id"] + suffix,
                "timestamp": (epoch + timedelta(hours=r["step"] - 1)).isoformat().replace("+00:00", "Z"),
                "type": r["type"], "amount": r["amount"], "orig_account_id": r["orig_id"],
                "orig_balance_before": r["orig_balance_before"], "dest_account_id": r["dest_id"],
                "dest_type": r["dest_type"]}

    with httpx.Client(base_url=args.base_url, timeout=10) as c:
        show("liveness", c.get("/health/live"))
        show("readiness", c.get("/health/ready"))
        show("loaded model", c.get("/v1/model", headers=h),
             ["model_version", "policy_version", "review_threshold", "decline_threshold"])

        show("no API key -> 401", c.post("/v1/score", json=body(legit.row(0, named=True), f"-{run}-x")))
        bad = body(legit.row(0, named=True), f"-{run}-bad") | {"amount": -5, "type": "WIRE"}
        show("invalid request -> 422", c.post("/v1/score", json=bad, headers=h))

        print("\n== PaySim test-period rows labelled fraud (label not sent to the API)")
        for r in fraud.iter_rows(named=True):
            res = c.post("/v1/score", json=body(r, f"-{run}"), headers=h)
            b = res.json()
            print(f"   HTTP {res.status_code} {b['transaction_id']}: {r['type']} amount={r['amount']:,.2f} "
                  f"-> p={b['fraud_probability']:.4f} {b['decision']} (features {b['latency']['feature_ms']} ms)")

        first = legit.row(0, named=True)
        req = body(first, f"-{run}")
        a = show("score a legitimate row", c.post("/v1/score", json=req, headers=h),
                 ["transaction_id", "fraud_probability", "decision", "risk_level", "model_version",
                  "idempotent_replay", "latency"])
        show("identical retry -> 200 replay", c.post("/v1/score", json=req, headers=h),
             ["fraud_probability", "decision", "idempotent_replay"])
        show("same id, different amount -> 409", c.post("/v1/score", json=req | {"amount": 1.0}, headers=h))
        show("stored transaction (masked)", c.get(f"/v1/transactions/{a['transaction_id']}", headers=h))

        label = {"transaction_id": a["transaction_id"], "is_fraud": False, "source": "simulation",
                 "label_time": (epoch + timedelta(hours=first["step"] + 24)).isoformat().replace("+00:00", "Z")}
        show("label -> 201", c.post("/v1/labels", json=label, headers=h))
        show("same label -> 200 replay", c.post("/v1/labels", json=label, headers=h))
        show("conflicting label -> 409", c.post("/v1/labels", json=label | {"is_fraud": True}, headers=h))
        page = show("review/decline queue", c.get("/v1/predictions", params={"decision": "DECLINE", "limit": 3},
                                                  headers=h), ["next_cursor"])
        print(f"   items: {[i['transaction_id'] for i in c.get('/v1/predictions', params={'decision': 'DECLINE', 'limit': 3}, headers=h).json()['items']]}")
        _ = page

        print(f"\n== sequential latency sample: {len(legit) - 1} new rows, one client, same machine")
        lat = []
        for r in legit.iter_rows(named=True):
            if r["transaction_id"] == first["transaction_id"]:
                continue
            t = time.perf_counter()
            res = c.post("/v1/score", json=body(r, f"-{run}"), headers=h)
            lat.append((time.perf_counter() - t) * 1000)
            assert res.status_code == 201, res.text
        q = statistics.quantiles(lat, n=100)
        print(f"   client-side ms: p50={q[49]:.1f} p95={q[94]:.1f} p99={q[98]:.1f} max={max(lat):.1f}")

        m = c.get("/metrics").text
        print("\n== selected metrics")
        for line in m.splitlines():
            if line.startswith(("riskstream_http_requests_total{", "riskstream_decisions_total",
                                "riskstream_idempotent_replays_total", "riskstream_conflicts_total",
                                "riskstream_auth_failures_total", "riskstream_model_loaded")):
                print("   " + line)


if __name__ == "__main__":
    main()
