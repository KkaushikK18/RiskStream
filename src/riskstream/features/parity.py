"""Offline/online feature parity check.

Replays transactions in event order through ``EntityHistory``. For each one,
features are computed from history *before* applying the event, then compared
with the offline table. Both must agree within floating-point tolerance.
"""

from __future__ import annotations

import math

import polars as pl

from riskstream.features.online import EntityHistory, online_features
from riskstream.features.registry import FeatureRegistry


def compare(canonical: pl.DataFrame, offline: pl.DataFrame, registry: FeatureRegistry,
            rtol: float = 1e-5, atol: float = 1e-4) -> dict:
    names = registry.all_names()
    history = EntityHistory(registry.config.windows)
    off = {r["transaction_id"]: r for r in offline.iter_rows(named=True)}
    mismatches: list[dict] = []
    max_abs = 0.0
    for txn in canonical.sort("step", "event_seq").iter_rows(named=True):
        on = online_features(history, txn, names)
        ref = off[txn["transaction_id"]]
        for n in names:
            a, b = on[n], ref[n]
            if math.isnan(a) and math.isnan(b):
                continue
            if math.isnan(a) != math.isnan(b) or not math.isclose(a, b, rel_tol=rtol, abs_tol=atol):
                mismatches.append({"transaction_id": txn["transaction_id"], "feature": n,
                                   "online": a, "offline": b})
            else:
                max_abs = max(max_abs, abs(a - b))
        history.update(txn["transaction_id"], txn["dest_id"], txn["orig_id"], txn["step"],
                       txn["amount"], txn["type"])
    return {"rows": len(canonical), "features": len(names), "mismatches": mismatches,
            "max_abs_diff": max_abs}
