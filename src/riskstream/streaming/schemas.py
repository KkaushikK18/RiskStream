"""Versioned event schemas. Consumers validate every message; a message that
fails validation is a permanent error and goes to the dead-letter topic."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from riskstream.api.schemas.common import IDENTIFIER

SCHEMA_VERSION = 1


class EventBase(BaseModel):
    # extra="ignore": producers may add fields in a backward-compatible version.
    model_config = ConfigDict(extra="ignore")
    event_id: str = Field(min_length=8, max_length=64)
    schema_version: int = Field(ge=1, le=SCHEMA_VERSION)


class IngestedTransaction(EventBase):
    """A transaction to be scored (from the replay tool or another producer)."""

    event_type: Literal["transaction.ingested"] = "transaction.ingested"
    transaction_id: str = Field(pattern=IDENTIFIER)
    timestamp: AwareDatetime
    type: Literal["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
    amount: float = Field(ge=0, allow_inf_nan=False)
    orig_account_id: str = Field(pattern=IDENTIFIER)
    orig_balance_before: float = Field(ge=0, allow_inf_nan=False)
    dest_account_id: str = Field(pattern=IDENTIFIER)
    dest_type: Literal["CUSTOMER", "MERCHANT"]
    # Simulation metadata, never sent to the API.
    replay_run_id: str | None = None

    def score_request(self) -> dict:
        return self.model_dump(mode="json", include={
            "transaction_id", "timestamp", "type", "amount", "orig_account_id", "orig_balance_before",
            "dest_account_id", "dest_type"})


class ScoredTransaction(EventBase):
    """Written by the API into the outbox; published by the relay."""

    transaction_id: str
    event_time: datetime
    step: int = Field(ge=1)
    type: str
    amount: float = Field(ge=0, allow_inf_nan=False)
    orig_account_id: str
    dest_account_id: str
    dest_type: str
    fraud_probability: float
    decision: str
    model_version: str
    policy_version: str
    scored_at: datetime


class LabeledTransaction(EventBase):
    transaction_id: str
    is_fraud: bool
    source: str
    label_time: datetime
    available_at: datetime


class InvalidEvent(ValueError):
    pass


def parse(model: type[EventBase], raw: bytes | None) -> EventBase:
    if raw is None:
        raise InvalidEvent("empty message value")
    try:
        return model.model_validate(json.loads(raw))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidEvent(f"not JSON: {exc}") from exc
    except ValidationError as exc:
        fields = ", ".join(".".join(str(p) for p in e["loc"]) + f" ({e['type']})" for e in exc.errors()[:5])
        raise InvalidEvent(f"schema validation failed: {fields}") from exc


def dumps(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"), default=str).encode()
