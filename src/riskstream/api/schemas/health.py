from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class DependencyStatus(BaseModel):
    name: str
    essential: bool
    status: Literal["ok", "degraded", "down"]
    detail: str | None = None
    latency_ms: float | None = None


class ReadinessResponse(BaseModel):
    status: Literal["ready", "degraded", "not_ready"]
    instance_id: str
    model_version: str | None
    policy_version: str | None
    dependencies: list[DependencyStatus]


class LivenessResponse(BaseModel):
    status: Literal["alive"]
    instance_id: str


class ModelInfo(BaseModel):
    model_version: str
    model_name: str
    feature_version: str
    feature_set: str
    n_features: int
    calibration_method: str
    policy_version: str
    policy_approval_status: str
    review_threshold: float
    decline_threshold: float
    trained_on_dataset: str
    is_synthetic_training_data: bool
    mlflow_run_id: str | None
