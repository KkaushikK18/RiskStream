"""Scoring and labeling service (no HTTP types here; routes translate errors).

Score flow for one request:

1. Idempotency check by ``transaction_id``: a stored result for an identical
   request is returned as is; the same id with different content is a 409.
2. Feature primitives from the entity's stored history (earlier hours only).
3. Shared ``derive`` formulas -> feature vector in the model's column order.
4. Calibrated probability -> decision via the loaded policy.
5. One database transaction writes the transaction, the prediction and a
   ``transactions.scored`` outbox event. Either all three commit or none do.
   If a concurrent identical request committed first (unique-key race), its
   stored result is returned.

Consistency: a scored transaction is visible to feature queries as soon as
step 5 commits. Because features only use earlier hours, two transactions in
the same hour never affect each other, so concurrent requests for one entity
need no locking.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from riskstream.api.schemas.scoring import LabelRequest, ScoreRequest
from riskstream.config import AppConfig
from riskstream.features.online import features_from_primitives
from riskstream.features.registry import FeatureRegistry
from riskstream.inference.model_loader import LoadedModel
from riskstream.logging_config import get_logger
from riskstream.monitoring import metrics
from riskstream.storage.database import session_scope, utc, utcnow
from riskstream.storage.models import LabelRow, PredictionRow, TransactionRow
from riskstream.storage.repositories import transactions as txn_repo
from riskstream.storage.repositories.events import (
    TOPIC_LABELED,
    TOPIC_SCORED,
    add_audit_event,
    add_outbox_event,
)
from riskstream.storage.repositories.feature_history import history_primitives

logger = get_logger(__name__)

# Labels may not claim to come from the future (beyond small clock skew).
LABEL_CLOCK_SKEW = timedelta(minutes=5)


class ServiceError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def request_hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def timestamp_to_step(ts: datetime, epoch: datetime) -> int:
    seconds = (ts.astimezone(UTC) - epoch).total_seconds()
    if seconds < 0:
        raise ServiceError(422, "timestamp_before_epoch",
                           f"timestamp is before the simulation epoch {epoch.isoformat()}")
    return int(seconds // 3600) + 1


def _json_safe(features: dict[str, float]) -> dict[str, float | None]:
    return {k: (None if v is None or (isinstance(v, float) and math.isnan(v)) else float(v))
            for k, v in features.items()}


@dataclass
class ScoreResult:
    transaction_id: str
    fraud_probability: float
    risk_level: str
    decision: str
    model_version: str
    policy_version: str
    feature_version: str
    scored_at: datetime
    idempotent_replay: bool
    feature_ms: float
    inference_ms: float
    total_ms: float


@dataclass
class LabelResult:
    row: LabelRow
    idempotent_replay: bool


class ScoringService:
    def __init__(self, sessions: sessionmaker[Session], model: LoadedModel, registry: FeatureRegistry,
                 cfg: AppConfig, instance_id: str) -> None:
        self.sessions, self.model, self.registry = sessions, model, registry
        self.cfg, self.instance_id = cfg, instance_id
        self.windows = registry.config.windows

    # ------------------------------------------------------------------ score

    def _replay(self, pred: PredictionRow, start: float) -> ScoreResult:
        metrics.IDEMPOTENT_REPLAYS.labels("score").inc()
        return ScoreResult(
            pred.transaction_id, pred.fraud_probability, pred.risk_level, pred.decision, pred.model_version,
            pred.policy_version, pred.feature_version, utc(pred.scored_at), True,
            pred.feature_latency_ms, pred.inference_latency_ms, (time.perf_counter() - start) * 1000,
        )

    def _existing(self, session: Session, transaction_id: str, digest: str, start: float) -> ScoreResult | None:
        existing = txn_repo.get_transaction(session, transaction_id)
        if existing is None:
            return None
        pred = txn_repo.get_prediction(session, transaction_id)
        if existing.request_sha256 == digest and pred is not None:
            return self._replay(pred, start)
        metrics.CONFLICTS.labels("score").inc()
        raise ServiceError(409, "transaction_id_conflict",
                           "transaction_id already exists with different content")

    def score(self, req: ScoreRequest, actor: str) -> ScoreResult:
        start = time.perf_counter()
        payload = req.model_dump(mode="json")
        digest = request_hash(payload)
        step = timestamp_to_step(req.timestamp, self.cfg.simulation_epoch)
        # Money is stored as NUMERIC(18, 2); round first so features see exactly what history will hold.
        req = req.model_copy(update={"amount": round(req.amount, 2),
                                     "orig_balance_before": round(req.orig_balance_before, 2)})
        txn = {"step": step, "type": req.type, "amount": req.amount, "dest_type": req.dest_type,
               "orig_balance_before": req.orig_balance_before}

        with session_scope(self.sessions) as session:
            t0 = time.perf_counter()
            replay = self._existing(session, req.transaction_id, digest, start)
            if replay is not None:
                return replay
            metrics.SCORE_STAGE.labels("idempotency_check").observe(time.perf_counter() - t0)

            t1 = time.perf_counter()
            prims = history_primitives(session, req.dest_account_id, req.orig_account_id, step, self.windows)
            t_hist = time.perf_counter()
            features = features_from_primitives(txn, prims, self.model.feature_names, self.windows)
            t2 = time.perf_counter()
            probability = self.model.predict_one(features)
            t3 = time.perf_counter()
            decision = self.model.policy.decide(probability)
            metrics.SCORE_STAGE.labels("history_query").observe(t_hist - t1)
            metrics.SCORE_STAGE.labels("feature_derivation").observe(t2 - t_hist)
            metrics.SCORE_STAGE.labels("model_inference").observe(t3 - t2)
            feature_ms, inference_ms = (t2 - t1) * 1000, (t3 - t2) * 1000
            scored_at = utcnow()
            t4 = time.perf_counter()
            try:
                self._persist(session, req, digest, step, probability, decision, features, feature_ms,
                              inference_ms, scored_at)
            except IntegrityError:
                # A concurrent request with the same transaction_id committed first.
                session.rollback()
                replay = self._existing(session, req.transaction_id, digest, start)
                if replay is not None:
                    return replay
                raise
            metrics.SCORE_STAGE.labels("persist").observe(time.perf_counter() - t4)

        metrics.DECISIONS.labels(decision.decision, self.model.model_version).inc()
        metrics.FRAUD_PROBABILITY.labels(self.model.model_version).observe(probability)
        total_ms = (time.perf_counter() - start) * 1000
        logger.info("scored %s -> %s", req.transaction_id, decision.decision,
                    extra={"event": "transaction_scored", "transaction_id": req.transaction_id,
                           "decision": decision.decision, "model_version": self.model.model_version,
                           "feature_ms": round(feature_ms, 2), "inference_ms": round(inference_ms, 2)})
        return ScoreResult(req.transaction_id, probability, decision.risk_level, decision.decision,
                           self.model.model_version, self.model.policy.policy_version, self.model.feature_version,
                           scored_at, False, feature_ms, inference_ms, total_ms)

    def _persist(self, session: Session, req: ScoreRequest, digest: str, step: int, probability: float,
                 decision, features: dict[str, float], feature_ms: float, inference_ms: float,
                 scored_at: datetime) -> None:
        """Transaction + prediction + outbox event in one database transaction."""
        session.add(TransactionRow(
            transaction_id=req.transaction_id, event_time=req.timestamp.astimezone(UTC), step=step,
            type=req.type, amount=Decimal(str(req.amount)), log_amount=math.log1p(req.amount),
            orig_id=req.orig_account_id, orig_balance_before=Decimal(str(req.orig_balance_before)),
            dest_id=req.dest_account_id, dest_type=req.dest_type, source="api", request_sha256=digest,
        ))
        session.flush()  # transaction row first: predictions has a foreign key to it
        session.add(PredictionRow(
            transaction_id=req.transaction_id, role="champion", model_version=self.model.model_version,
            policy_version=self.model.policy.policy_version, feature_version=self.model.feature_version,
            fraud_probability=probability, risk_level=decision.risk_level, decision=decision.decision,
            features=_json_safe(features), feature_latency_ms=feature_ms, inference_latency_ms=inference_ms,
            instance_id=self.instance_id, scored_at=scored_at,
        ))
        add_outbox_event(session, TOPIC_SCORED, req.dest_account_id, {
            "transaction_id": req.transaction_id, "event_time": req.timestamp.astimezone(UTC).isoformat(),
            "step": step, "type": req.type, "amount": req.amount, "orig_account_id": req.orig_account_id,
            "dest_account_id": req.dest_account_id, "dest_type": req.dest_type,
            "fraud_probability": probability, "decision": decision.decision,
            "model_version": self.model.model_version, "policy_version": self.model.policy.policy_version,
            "scored_at": scored_at.isoformat(),
        })
        session.commit()

    # ------------------------------------------------------------------ labels

    def record_label(self, req: LabelRequest, actor: str) -> LabelResult:
        digest = request_hash(req.model_dump(mode="json"))
        now = utcnow()
        label_time = req.label_time.astimezone(UTC)
        with session_scope(self.sessions) as session:
            txn = txn_repo.get_transaction(session, req.transaction_id)
            if txn is None:
                raise ServiceError(404, "transaction_not_found", "No transaction with this id")
            existing = txn_repo.get_label(session, req.transaction_id)
            if existing is not None:
                return self._label_replay_or_conflict(existing, digest)
            if label_time < utc(txn.event_time):
                raise ServiceError(422, "label_before_transaction", "label_time is earlier than the transaction")
            if label_time > now + LABEL_CLOCK_SKEW:
                raise ServiceError(422, "label_in_future", "label_time is in the future")
            row = LabelRow(transaction_id=req.transaction_id, is_fraud=req.is_fraud, source=req.source,
                           label_time=label_time, available_at=now, submitted_by=actor, request_sha256=digest)
            session.add(row)
            add_outbox_event(session, TOPIC_LABELED, txn.dest_id, {
                "transaction_id": req.transaction_id, "is_fraud": req.is_fraud, "source": req.source,
                "label_time": label_time.isoformat(), "available_at": now.isoformat(),
            })
            add_audit_event(session, "label_recorded", actor, req.transaction_id,
                            {"is_fraud": req.is_fraud, "source": req.source})
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                existing = txn_repo.get_label(session, req.transaction_id)
                if existing is None:
                    raise
                return self._label_replay_or_conflict(existing, digest)
        metrics.LABELS.labels(req.source, str(req.is_fraud).lower()).inc()
        return LabelResult(row, False)

    @staticmethod
    def _label_replay_or_conflict(existing: LabelRow, digest: str) -> LabelResult:
        if existing.request_sha256 == digest:
            metrics.IDEMPOTENT_REPLAYS.labels("labels").inc()
            return LabelResult(existing, True)
        metrics.CONFLICTS.labels("labels").inc()
        raise ServiceError(409, "label_conflict",
                           "A different label already exists for this transaction; labels are never overwritten")
