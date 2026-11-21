"""Model deployment records."""

from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from riskstream.storage.database import utcnow
from riskstream.storage.models import ModelDeploymentRow
from riskstream.storage.repositories.events import add_audit_event


def active_deployment(session: Session) -> ModelDeploymentRow | None:
    return session.execute(
        select(ModelDeploymentRow)
        .where(ModelDeploymentRow.status == "active")
        .order_by(ModelDeploymentRow.id.desc())
        .limit(1)
    ).scalar_one_or_none()


def record_loaded_model(session: Session, *, model_version: str, policy_version: str,
                        feature_version: str, bundle_path: str, policy_path: str, actor: str) -> bool:
    """Make (model, policy) the active deployment if it is not already.

    Returns True when a new deployment row was written. Every call also writes a
    ``model_loaded`` audit event so each instance start is traceable.
    """
    current = active_deployment(session)
    changed = current is None or (current.model_version, current.policy_version) != (
        model_version, policy_version)
    now = utcnow()
    if changed:
        session.execute(
            update(ModelDeploymentRow)
            .where(ModelDeploymentRow.status == "active")
            .values(status="inactive", deactivated_at=now)
        )
        session.add(ModelDeploymentRow(
            model_version=model_version, policy_version=policy_version, feature_version=feature_version,
            bundle_path=bundle_path, policy_path=policy_path, status="active", activated_at=now,
            activated_by=actor,
        ))
        add_audit_event(session, "model_activated", actor, model_version, {
            "policy_version": policy_version,
            "previous_model_version": current.model_version if current else None,
            "previous_policy_version": current.policy_version if current else None,
        })
    add_audit_event(session, "model_loaded", actor, model_version,
                    {"policy_version": policy_version, "feature_version": feature_version})
    return changed
