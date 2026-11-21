"""Live HTTP walkthrough against a running RiskStream API.

    python -m riskstream.cli serve            # in another terminal
    python scripts/demo_api.py [--steps 1] [--base-url http://127.0.0.1:8000]

The demo plays the NEXT simulated hour(s) of PaySim traffic after the stored
history: every transaction of those steps, in event order, with its real id.
Each scored request then sees exactly the history the model saw in training,
and the stored history remains an exact prefix of the dataset. Running the
demo again plays the following hour.

It refuses to run if the stored history has gaps against the dataset
(see `python -m riskstream.cli history-status`).

Reads the database (MIGRATION_DATABASE_URL) only to find the latest stored
step. All scoring goes through HTTP. Single-client timings, not a benchmark.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from datetime import timedelta
from pathlib import Path

import httpx
import polars as pl

from riskstream.config import PROJECT_ROOT, AppConfig, get_settings
from riskstream.data.ingestion import load_interim
from riskstream.storage.admin import admin_engine, history_gaps, latest_stored_step


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
    ap.add_argument("--steps", type=int, default=1, help="Simulated hours to play")
    args = ap.parse_args()
    key = Path(args.key_file).read_text().strip()
    h = {"X-API-Key": key}
    epoch = AppConfig.load().simulation_epoch
    canonical = load_interim("paysim")

    engine = admin_engine(get_settings().admin_database_url)
    try:
        latest = latest_stored_step(engine) or 0
        gaps = history_gaps(engine, canonical, latest) if latest else []
    finally:
        engine.dispose()
    if gaps:
        raise SystemExit(f"Stored history differs from the dataset at steps {gaps[:10]}... "
                         "Fix it first (history-status / delete-scored / seed-history).")
    first_step, last_step = latest + 1, latest + args.steps
    rows = (canonical.filter(pl.col("step").is_between(first_step, last_step))
            .sort("step", "event_seq").collect())
    if len(rows) == 0:
        raise SystemExit(f"No dataset rows in steps {first_step}-{last_step}")
    print(f"Stored history: steps 1-{latest}, no gaps. Playing steps {first_step}-{last_step}: "
          f"{len(rows):,} transactions ({int(rows['is_fraud'].sum())} labelled fraud in the dataset; "
          "labels are not sent).")

    def body(r: dict) -> dict:
        return {"transaction_id": r["transaction_id"],
                "timestamp": (epoch + timedelta(hours=r["step"] - 1)).isoformat().replace("+00:00", "Z"),
                "type": r["type"], "amount": r["amount"], "orig_account_id": r["orig_id"],
                "orig_balance_before": r["orig_balance_before"], "dest_account_id": r["dest_id"],
                "dest_type": r["dest_type"]}

    first = rows.row(0, named=True)
    with httpx.Client(base_url=args.base_url, timeout=10) as c:
        show("liveness", c.get("/health/live"))
        show("readiness", c.get("/health/ready"), ["status", "model_version", "policy_version"])
        show("loaded model", c.get("/v1/model", headers=h),
             ["model_version", "policy_version", "review_threshold", "decline_threshold"])
        # Neither request below writes anything.
        show("no API key -> 401", c.post("/v1/score", json=body(first)))
        show("invalid request -> 422", c.post("/v1/score", json=body(first) | {"amount": -5, "type": "WIRE"},
                                               headers=h))

        print(f"\n== scoring all {len(rows):,} transactions of steps {first_step}-{last_step} in event order")
        lat, decisions, fraud_lines = [], {}, []
        confusion = {"fraud_flagged": 0, "fraud_approved": 0, "legit_flagged": 0, "legit_approved": 0}
        for r in rows.iter_rows(named=True):
            t = time.perf_counter()
            res = c.post("/v1/score", json=body(r), headers=h)
            lat.append((time.perf_counter() - t) * 1000)
            if res.status_code not in (200, 201):
                raise SystemExit(f"{r['transaction_id']}: HTTP {res.status_code} {res.text}")
            b = res.json()
            decisions[b["decision"]] = decisions.get(b["decision"], 0) + 1
            flagged = b["decision"] != "APPROVE"
            truth = "fraud" if r["is_fraud"] else "legit"
            confusion[f"{truth}_{'flagged' if flagged else 'approved'}"] += 1
            if r["is_fraud"] and len(fraud_lines) < 5:
                fraud_lines.append(f"   {b['transaction_id']}: {r['type']} amount={r['amount']:,.2f} "
                                   f"-> p={b['fraud_probability']:.4f} {b['decision']}")
        print(f"   decisions: {decisions}")
        print(f"   against dataset labels: {confusion}")
        print("   first fraud rows:" if fraud_lines else "   (no fraud rows in these steps)")
        print("\n".join(fraud_lines))
        q = statistics.quantiles(lat, n=100)
        print(f"   client-side ms: p50={q[49]:.1f} p95={q[94]:.1f} p99={q[98]:.1f} max={max(lat):.1f}")

        req = body(first)
        show("identical retry -> 200 replay", c.post("/v1/score", json=req, headers=h),
             ["transaction_id", "fraud_probability", "decision", "idempotent_replay"])
        show("same id, different amount -> 409", c.post("/v1/score", json=req | {"amount": 1.0}, headers=h))
        show("stored transaction (masked)", c.get(f"/v1/transactions/{first['transaction_id']}", headers=h))

        label = {"transaction_id": first["transaction_id"], "is_fraud": bool(first["is_fraud"]),
                 "source": "simulation",
                 "label_time": (epoch + timedelta(hours=first["step"] + 24)).isoformat().replace("+00:00", "Z")}
        show("label (dataset's true outcome) -> 201", c.post("/v1/labels", json=label, headers=h))
        show("same label -> 200 replay", c.post("/v1/labels", json=label, headers=h))
        show("conflicting label -> 409", c.post("/v1/labels", json=label | {"is_fraud": not label["is_fraud"]},
                                                headers=h))
        page = c.get("/v1/predictions", params={"decision": "DECLINE", "limit": 3}, headers=h).json()
        print(f"\n== latest DECLINE decisions: {[i['transaction_id'] for i in page['items']]}")

        print("\n== selected metrics")
        for line in c.get("/metrics").text.splitlines():
            if line.startswith(("riskstream_http_requests_total{method=\"POST\"", "riskstream_decisions_total",
                                "riskstream_idempotent_replays_total", "riskstream_conflicts_total")):
                print("   " + line)
    print(f"\nNext run will play step {last_step + 1}.")


if __name__ == "__main__":
    main()
