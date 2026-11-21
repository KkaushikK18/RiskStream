"""Scoring, transaction lookup and label schemas.

Field names are RiskStream's canonical names, not PaySim's source columns.
Mapping from PaySim: nameOrig -> orig_account_id, oldbalanceOrg ->
orig_balance_before, nameDest -> dest_account_id, and the destination's
``M``/``C`` id prefix -> dest_type. ``timestamp`` maps to PaySim's hourly
``step`` through ``simulation.epoch`` in ``configs/app.yaml``.

Post-transaction balances and the destination's balances are deliberately not
accepted: the model never uses them (docs/decisions/0004).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import AwareDatetime, Field

from riskstream.api.schemas.common import IDENTIFIER, StrictModel

TransactionType = Literal["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
DestType = Literal["CUSTOMER", "MERCHANT"]
DecisionValue = Literal["APPROVE", "REVIEW", "DECLINE"]
RiskLevel = Literal["LOW", "MEDIUM", "HIGH"]
LabelSource = Literal["chargeback", "analyst_review", "customer_report", "simulation"]

MAX_AMOUNT = 100_000_000.0  # same bound as configs/data.yaml quality.amount_max


class ScoreRequest(StrictModel):
    transaction_id: str = Field(pattern=IDENTIFIER, description="Client-unique id; also the idempotency key")
    timestamp: AwareDatetime = Field(description="Event time with timezone (ISO 8601)")
    type: TransactionType
    amount: float = Field(ge=0, le=MAX_AMOUNT, allow_inf_nan=False)
    orig_account_id: str = Field(pattern=IDENTIFIER)
    orig_balance_before: float = Field(ge=0, le=1e12, allow_inf_nan=False,
                                       description="Sender balance before the transaction")
    dest_account_id: str = Field(pattern=IDENTIFIER)
    dest_type: DestType

    model_config = StrictModel.model_config | {
        "json_schema_extra": {
            "examples": [{
                "transaction_id": "txn_10001",
                "timestamp": "2026-01-25T10:30:00Z",
                "type": "TRANSFER",
                "amount": 4500.0,
                "orig_account_id": "C1231006815",
                "orig_balance_before": 4500.0,
                "dest_account_id": "C553264065",
                "dest_type": "CUSTOMER",
            }]
        }
    }


class Latency(StrictModel):
    feature_ms: float = Field(description="Feature-history lookup + feature derivation")
    inference_ms: float = Field(description="Model + calibrator prediction")
    total_ms: float = Field(description="Server-side time for the whole request")


class ScoreResponse(StrictModel):
    transaction_id: str
    fraud_probability: float = Field(ge=0, le=1, description="Calibrated probability")
    risk_level: RiskLevel
    decision: DecisionValue
    model_version: str
    policy_version: str
    feature_version: str
    scored_at: datetime
    idempotent_replay: bool = Field(description="True when this is a stored result for a repeated request")
    latency: Latency


class TransactionView(StrictModel):
    transaction_id: str
    timestamp: datetime
    step: int
    type: TransactionType
    amount: float
    orig_account: str = Field(description="Masked account id")
    dest_account: str = Field(description="Masked account id")
    dest_type: DestType


class PredictionView(StrictModel):
    fraud_probability: float
    risk_level: RiskLevel
    decision: DecisionValue
    model_version: str
    policy_version: str
    feature_version: str
    scored_at: datetime


class LabelView(StrictModel):
    is_fraud: bool
    source: LabelSource
    label_time: datetime
    available_at: datetime


class TransactionDetail(StrictModel):
    transaction: TransactionView
    prediction: PredictionView | None
    label: LabelView | None


class PredictionListItem(StrictModel):
    transaction_id: str
    timestamp: datetime
    type: TransactionType
    amount: float
    prediction: PredictionView


class PredictionPage(StrictModel):
    items: list[PredictionListItem]
    next_cursor: str | None = Field(description="Pass as `cursor` to fetch the next (older) page")


class LabelRequest(StrictModel):
    transaction_id: str = Field(pattern=IDENTIFIER)
    is_fraud: bool
    source: LabelSource
    label_time: AwareDatetime = Field(description="When the outcome was determined")


class LabelResponse(StrictModel):
    transaction_id: str
    is_fraud: bool
    source: LabelSource
    label_time: datetime
    available_at: datetime
    idempotent_replay: bool
