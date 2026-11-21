"""Outbox and audit events. Both are added to the caller's session, so they commit
or roll back together with the state change they describe."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from riskstream.storage.database import utcnow
from riskstream.storage.models import AuditEventRow, OutboxEventRow

TOPIC_SCORED = "transactions.scored"
TOPIC_LABELED = "transactions.labeled"
EVENT_SCHEMA_VERSION = 1


def add_outbox_event(session: Session, topic: str, key: str, payload: dict[str, Any]) -> OutboxEventRow:
    event_id = str(uuid.uuid4())
    row = OutboxEventRow(
        event_id=event_id,
        topic=topic,
        event_key=key,
        payload={"event_id": event_id, "schema_version": EVENT_SCHEMA_VERSION, **payload},
        created_at=utcnow(),
        attempts=0,
    )
    session.add(row)
    return row


def add_audit_event(session: Session, event_type: str, actor: str, subject: str,
                    details: dict[str, Any]) -> AuditEventRow:
    row = AuditEventRow(event_type=event_type, actor=actor, subject=subject, details=details,
                        created_at=utcnow())
    session.add(row)
    return row
