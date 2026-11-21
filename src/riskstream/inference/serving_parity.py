"""Training/serving skew check.

Scores real dataset transactions through the production scoring path
(PostgreSQL history -> shared derive -> model -> policy -> persistence) and
compares each served feature vector and probability with the offline
feature table and the bundle's offline prediction.

Preconditions (checked): for every step before ``start_step`` the database
holds exactly the dataset's rows, and nothing at or after ``start_step``.
Transactions of ``start_step .. start_step + steps - 1`` are then scored in
event order, so each one sees exactly the history the offline pipeline used.

By default everything runs inside one database transaction that is rolled
back at the end. Each request's commit becomes a SAVEPOINT release, so later
rows still see earlier ones, but the database is unchanged afterwards and the
check can be repeated on the same steps. ``commit=True`` keeps the rows.
"""

from __future__ import annotations

import math
import time
from datetime import timedelta

import numpy as np
import polars as pl
from sqlalchemy import Engine, exists, select, text
from sqlalchemy.orm import sessionmaker

from riskstream.api.schemas.scoring import ScoreRequest
from riskstream.data.ingestion import load_interim
from riskstream.data.splitting import SPLITS
from riskstream.features.offline import load_features
from riskstream.inference.model_loader import LoadedModel
from riskstream.inference.predictor import ScoringService
from riskstream.logging_config import get_logger
from riskstream.storage.admin import history_gaps, latest_stored_step
from riskstream.storage.database import session_factory, session_scope
from riskstream.storage.models import PredictionRow, TransactionRow
from riskstream.storage.repositories.transactions import get_prediction

logger = get_logger(__name__)


def _offline_rows(dataset: str, ids: list[str]) -> pl.DataFrame:
    parts = [load_features(dataset, s).filter(pl.col("transaction_id").is_in(ids)) for s in SPLITS]
    return pl.concat([p for p in parts if len(p)])


def _check_preconditions(engine: Engine, canonical: pl.LazyFrame, start_step: int) -> None:
    with engine.connect() as conn, conn.begin():
        if engine.dialect.name == "postgresql":
            # Admin check over the whole table. SET LOCAL ends with this transaction, so the
            # pooled connection goes back with the API's normal statement timeout.
            conn.execute(text("SET LOCAL statement_timeout = '120s'"))
        later = conn.execute(select(exists().where(TransactionRow.step >= start_step))).scalar_one()
        if later:
            raise RuntimeError(f"rows already stored at step >= {start_step}. "
                               f"Latest stored step: {latest_stored_step(conn)}")
        gaps = history_gaps(conn, canonical, start_step - 1)
    if gaps:
        shown = gaps[:10]
        raise RuntimeError(f"stored history differs from the dataset at {len(gaps)} step(s), first: {shown}. "
                           f"Run seed-history to fill them")


def check_serving_parity(engine: Engine, model: LoadedModel, service_factory, dataset: str,
                         start_step: int | None, steps: int, limit: int | None = None,
                         commit: bool = False, atol: float = 1e-4, canonical: pl.LazyFrame | None = None,
                         offline_rows=None) -> dict:
    """``service_factory(sessions) -> ScoringService``; tests can inject data via
    ``canonical`` and ``offline_rows(ids) -> DataFrame``."""
    canonical = canonical if canonical is not None else load_interim(dataset)
    offline_rows = offline_rows or (lambda ids: _offline_rows(dataset, ids))
    if start_step is None:
        latest = latest_stored_step(engine)
        start_step = 1 if latest is None else latest + 1
    _check_preconditions(engine, canonical, start_step)

    rows = (canonical.filter(pl.col("step").is_between(start_step, start_step + steps - 1))
            .sort("step", "event_seq").collect())
    if len(rows) == 0:
        raise RuntimeError(f"dataset has no rows in steps {start_step}..{start_step + steps - 1}")
    if limit is not None:
        # Truncating inside a step is fine: same-step rows never feed each other's features.
        rows = rows.head(limit)
    offline = offline_rows(rows["transaction_id"].to_list())
    off = {r["transaction_id"]: r for r in offline.iter_rows(named=True)}
    names = model.feature_names
    X = offline.select(names).to_numpy().astype(np.float32)
    p_off = dict(zip(offline["transaction_id"].to_list(), model.bundle.predict_proba(X), strict=True))

    conn = engine.connect()
    outer = conn.begin()
    if commit:
        sessions = session_factory(engine)
    else:
        sessions = sessionmaker(bind=conn, expire_on_commit=False, autoflush=False,
                                join_transaction_mode="create_savepoint")
    service: ScoringService = service_factory(sessions)
    epoch = service.cfg.simulation_epoch

    feature_mismatches: list[dict] = []
    max_feat_diff = max_p_diff = 0.0
    decisions_equal = 0
    latencies = []
    t0 = time.perf_counter()
    try:
        for r in rows.iter_rows(named=True):
            req = ScoreRequest(
                transaction_id=r["transaction_id"], timestamp=epoch + timedelta(hours=r["step"] - 1),
                type=r["type"], amount=r["amount"], orig_account_id=r["orig_id"],
                orig_balance_before=r["orig_balance_before"], dest_account_id=r["dest_id"],
                dest_type=r["dest_type"],
            )
            res = service.score(req, "serving-parity-check")
            latencies.append(res.total_ms)
            with session_scope(sessions) as s:
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
            offline_p = float(p_off[r["transaction_id"]])
            max_p_diff = max(max_p_diff, abs(res.fraud_probability - offline_p))
            decisions_equal += int(model.policy.decide(offline_p).decision == res.decision)
    finally:
        outer.rollback()  # no-op for commit=True (nothing ran on this connection)
        conn.close()
    elapsed = time.perf_counter() - t0
    n = len(rows)
    return {
        "dataset": dataset, "start_step": start_step, "steps": steps, "rows_scored": n,
        "mode": "committed" if commit else "rolled_back",
        "features_compared": len(names) * n,
        "feature_mismatches": len(feature_mismatches), "mismatch_examples": feature_mismatches[:10],
        "max_abs_feature_diff": max_feat_diff, "max_abs_probability_diff": max_p_diff,
        "decisions_identical": decisions_equal, "seconds": round(elapsed, 2),
        "in_process_latency_ms": {"p50": float(np.percentile(latencies, 50)),
                                  "p95": float(np.percentile(latencies, 95)),
                                  "p99": float(np.percentile(latencies, 99))},
    }
