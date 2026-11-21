"""Training/serving skew check.

Scores real dataset transactions through the production scoring path
(PostgreSQL history -> shared derive -> model -> policy -> persistence) and
compares each served feature vector and probability with the offline
feature table and the bundle's offline prediction.

Preconditions (checked): the database holds backfilled history for steps
before ``start_step`` and no rows at or after it. Transactions of
``start_step .. start_step + steps - 1`` are then scored in event order, so
each one sees exactly the history the offline pipeline used.
"""

from __future__ import annotations

import math
import time
from datetime import timedelta

import numpy as np
import polars as pl
from sqlalchemy import exists, func, select, text

from riskstream.api.schemas.scoring import ScoreRequest
from riskstream.config import AppConfig
from riskstream.data.ingestion import load_interim
from riskstream.data.splitting import SPLITS
from riskstream.features.offline import load_features
from riskstream.inference.predictor import ScoringService
from riskstream.logging_config import get_logger
from riskstream.storage.database import session_scope
from riskstream.storage.models import PredictionRow, TransactionRow
from riskstream.storage.repositories.transactions import get_prediction

logger = get_logger(__name__)


def _offline_rows(dataset: str, ids: list[str]) -> pl.DataFrame:
    parts = [load_features(dataset, s).filter(pl.col("transaction_id").is_in(ids)) for s in SPLITS]
    return pl.concat([p for p in parts if len(p)])


def check_serving_parity(service: ScoringService, dataset: str, start_step: int, steps: int,
                         limit: int | None = None, atol: float = 1e-4) -> dict:
    cfg: AppConfig = service.cfg
    with session_scope(service.sessions) as s:
        # One-off admin check over the whole table: lift the API's statement timeout
        # for this transaction only (scoring queries keep the configured limit).
        if s.get_bind().dialect.name == "postgresql":
            s.execute(text("SET LOCAL statement_timeout = '120s'"))
        later = s.execute(select(exists().where(TransactionRow.step >= start_step))).scalar_one()
        earlier = s.execute(select(func.count()).select_from(TransactionRow)
                            .where(TransactionRow.step < start_step)).scalar_one()
        s.commit()
    if later:
        raise RuntimeError(f"transactions already stored at step >= {start_step}; use a fresh range")
    expected_history = load_interim(dataset).filter(pl.col("step") < start_step).select(pl.len()).collect().item()
    if earlier != expected_history:
        raise RuntimeError(f"history mismatch: database has {earlier:,} rows before step {start_step}, "
                           f"dataset has {expected_history:,}. Run seed-history --until-step {start_step - 1}")

    rows = (load_interim(dataset).filter(pl.col("step").is_between(start_step, start_step + steps - 1))
            .sort("step", "event_seq").collect())
    if limit is not None:
        # Truncating inside a step would leave later rows of the SAME step unscored, which is
        # fine: same-step rows never feed each other's features.
        rows = rows.head(limit)
    offline = _offline_rows(dataset, rows["transaction_id"].to_list())
    off = {r["transaction_id"]: r for r in offline.iter_rows(named=True)}
    names = service.model.feature_names
    X = offline.select(names).to_numpy().astype(np.float32)
    p_off = dict(zip(offline["transaction_id"].to_list(), service.model.bundle.predict_proba(X), strict=True))

    feature_mismatches: list[dict] = []
    max_feat_diff = max_p_diff = 0.0
    decisions_equal = 0
    latencies = []
    t0 = time.perf_counter()
    for r in rows.iter_rows(named=True):
        req = ScoreRequest(
            transaction_id=r["transaction_id"],
            timestamp=cfg.simulation_epoch + timedelta(hours=r["step"] - 1),
            type=r["type"], amount=r["amount"], orig_account_id=r["orig_id"],
            orig_balance_before=r["orig_balance_before"], dest_account_id=r["dest_id"], dest_type=r["dest_type"],
        )
        res = service.score(req, "serving-parity-check")
        latencies.append(res.total_ms)
        with session_scope(service.sessions) as s:
            pred: PredictionRow = get_prediction(s, r["transaction_id"])
            served = pred.features
        ref = off[r["transaction_id"]]
        for n in names:
            a = served[n]
            b = ref[n]
            a = float("nan") if a is None else a
            if math.isnan(a) and math.isnan(b):
                continue
            if math.isnan(a) != math.isnan(b) or abs(a - b) > atol * max(1.0, abs(b)):
                feature_mismatches.append({"transaction_id": r["transaction_id"], "feature": n,
                                           "served": a, "offline": b})
            else:
                max_feat_diff = max(max_feat_diff, abs(a - b))
        dp = abs(res.fraud_probability - float(p_off[r["transaction_id"]]))
        max_p_diff = max(max_p_diff, dp)
        offline_decision = service.model.policy.decide(float(p_off[r["transaction_id"]])).decision
        decisions_equal += int(offline_decision == res.decision)
    elapsed = time.perf_counter() - t0
    n = len(rows)
    return {
        "dataset": dataset, "start_step": start_step, "steps": steps, "rows_scored": n,
        "history_rows": earlier, "features_compared": len(names) * n,
        "feature_mismatches": len(feature_mismatches), "mismatch_examples": feature_mismatches[:10],
        "max_abs_feature_diff": max_feat_diff, "max_abs_probability_diff": max_p_diff,
        "decisions_identical": decisions_equal, "seconds": round(elapsed, 2),
        "in_process_latency_ms": {"p50": float(np.percentile(latencies, 50)) if latencies else None,
                                  "p95": float(np.percentile(latencies, 95)) if latencies else None,
                                  "p99": float(np.percentile(latencies, 99)) if latencies else None},
    }
