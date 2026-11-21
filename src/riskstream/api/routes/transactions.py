from __future__ import annotations

import base64
from typing import Literal

from fastapi import APIRouter, Depends, Path, Query, Request

from riskstream.api.dependencies import require_role
from riskstream.api.errors import AppError
from riskstream.api.schemas.common import IDENTIFIER, ErrorResponse
from riskstream.api.schemas.health import ModelInfo
from riskstream.api.schemas.scoring import (
    LabelView,
    PredictionListItem,
    PredictionPage,
    PredictionView,
    TransactionDetail,
    TransactionView,
)
from riskstream.api.security import Principal
from riskstream.storage.database import session_scope, utc
from riskstream.storage.models import LabelRow, PredictionRow, TransactionRow
from riskstream.storage.repositories import transactions as repo

router = APIRouter(prefix="/v1", tags=["transactions"])
ERRORS = {code: {"model": ErrorResponse} for code in (401, 403, 404, 422, 429, 503)}


def mask(account_id: str) -> str:
    """Show only the type prefix and last four characters."""
    if len(account_id) <= 5:
        return account_id[:1] + "***"
    return f"{account_id[:1]}***{account_id[-4:]}"


def _txn_view(t: TransactionRow) -> TransactionView:
    return TransactionView(transaction_id=t.transaction_id, timestamp=utc(t.event_time), step=t.step, type=t.type,
                           amount=float(t.amount), orig_account=mask(t.orig_id), dest_account=mask(t.dest_id),
                           dest_type=t.dest_type)


def _pred_view(p: PredictionRow) -> PredictionView:
    return PredictionView(fraud_probability=p.fraud_probability, risk_level=p.risk_level, decision=p.decision,
                          model_version=p.model_version, policy_version=p.policy_version,
                          feature_version=p.feature_version, scored_at=utc(p.scored_at))


def _label_view(lbl: LabelRow) -> LabelView:
    return LabelView(is_fraud=lbl.is_fraud, source=lbl.source, label_time=utc(lbl.label_time),
                     available_at=utc(lbl.available_at))


def _sessions(request: Request):
    if request.app.state.sessions is None:
        raise AppError(503, "service_unavailable", "Database is not configured", headers={"Retry-After": "5"})
    return request.app.state.sessions


@router.get("/transactions/{transaction_id}", response_model=TransactionDetail, responses=ERRORS,
            summary="Stored transaction, prediction and label")
def get_transaction(request: Request, transaction_id: str = Path(pattern=IDENTIFIER),
                    _: Principal = Depends(require_role("reader"))) -> TransactionDetail:
    with session_scope(_sessions(request)) as s:
        t = repo.get_transaction(s, transaction_id)
        if t is None:
            raise AppError(404, "transaction_not_found", "No transaction with this id")
        p = repo.get_prediction(s, transaction_id)
        lbl = repo.get_label(s, transaction_id)
        return TransactionDetail(transaction=_txn_view(t), prediction=_pred_view(p) if p else None,
                                 label=_label_view(lbl) if lbl else None)


def _encode_cursor(pred_id: int) -> str:
    return base64.urlsafe_b64encode(f"p:{pred_id}".encode()).decode().rstrip("=")


def _decode_cursor(cursor: str) -> int:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        prefix, value = raw.split(":", 1)
        if prefix != "p" or not value.isdigit():
            raise ValueError
        return int(value)
    except (ValueError, UnicodeDecodeError) as exc:
        raise AppError(422, "invalid_cursor", "cursor is not valid") from exc


@router.get("/predictions", response_model=PredictionPage, responses=ERRORS,
            summary="Recent predictions, newest first (e.g. the review queue)")
def list_predictions(
    request: Request,
    decision: Literal["APPROVE", "REVIEW", "DECLINE"] | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1),
    cursor: str | None = Query(default=None, max_length=64),
    _: Principal = Depends(require_role("reader")),
) -> PredictionPage:
    cfg = request.app.state.cfg
    limit = min(limit or cfg.page_size_default, cfg.page_size_max)
    before = _decode_cursor(cursor) if cursor else None
    with session_scope(_sessions(request)) as s:
        rows = repo.list_predictions(s, decision=decision, before_id=before, limit=limit + 1)
    page, more = rows[:limit], len(rows) > limit
    items = [PredictionListItem(transaction_id=t.transaction_id, timestamp=utc(t.event_time), type=t.type,
                                amount=float(t.amount), prediction=_pred_view(p)) for p, t in page]
    return PredictionPage(items=items, next_cursor=_encode_cursor(page[-1][0].id) if more and page else None)


@router.get("/model", response_model=ModelInfo, responses=ERRORS, tags=["operations"],
            summary="Loaded model and decision policy")
def model_info(request: Request, _: Principal = Depends(require_role("reader"))) -> ModelInfo:
    m = request.app.state.model
    if m is None:
        raise AppError(503, "model_unavailable", request.app.state.model_error or "No model loaded")
    meta = m.bundle.metadata
    return ModelInfo(
        model_version=m.model_version, model_name=meta["model_name"], feature_version=m.feature_version,
        feature_set=meta.get("feature_set", "full"), n_features=len(m.feature_names),
        calibration_method=meta["calibration_method"], policy_version=m.policy.policy_version,
        policy_approval_status=m.policy.approval_status, review_threshold=m.policy.review_threshold,
        decline_threshold=m.policy.decline_threshold, trained_on_dataset=meta["dataset"],
        is_synthetic_training_data=bool(meta.get("is_synthetic_data", False)), mlflow_run_id=meta.get("mlflow_run_id"),
    )
