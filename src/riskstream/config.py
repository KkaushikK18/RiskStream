"""Configuration management.

Two sources of configuration, each with one job:

* Environment variables (``.env``) hold deployment-level and secret settings:
  log level, database URLs, API key hashes, model artifact paths.
* YAML files in ``configs/`` hold versioned, reviewable settings: dataset
  paths, split boundaries, features, models, service limits.

All relative paths are resolved against the project root, so commands behave
the same regardless of the current working directory.
"""

from __future__ import annotations

import copy
import os
import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cache, lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
CONFIG_DIR: Path = PROJECT_ROOT / "configs"


class ApiKeyEntry(BaseModel):
    """One API client. Only the SHA-256 of the key is configured, never the key."""

    name: str = Field(pattern=r"^[A-Za-z0-9_.\-]{1,64}$")
    roles: list[str]
    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    rate_per_second: float | None = Field(default=None, gt=0)


def _default_instance_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


class Settings(BaseSettings):
    """Environment-level settings. Field names map to upper-case env vars."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: str = "development"
    log_level: str = "INFO"
    log_format: Literal["text", "json"] = "text"
    random_seed: int = 42
    mlflow_tracking_uri: str = f"sqlite:///{(PROJECT_ROOT / 'mlflow.db').as_posix()}"

    # Serving (Phase 3)
    database_url: str | None = None
    # Owner role used for migrations and bulk loads; falls back to database_url.
    migration_database_url: str | None = None
    model_bundle_path: Path | None = None
    policy_path: Path | None = None
    # JSON list of ApiKeyEntry objects.
    api_keys: list[ApiKeyEntry] = Field(default_factory=list)
    instance_id: str = Field(default_factory=_default_instance_id)

    @property
    def admin_database_url(self) -> str | None:
        return self.migration_database_url or self.database_url


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached environment settings."""
    return Settings()


def resolve_path(path: str | Path) -> Path:
    """Resolve a config path against the project root unless already absolute."""
    path = Path(path)
    return path if path.is_absolute() else PROJECT_ROOT / path


@cache
def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(name: str) -> dict[str, Any]:
    """Load ``configs/<name>.yaml``.

    Returns a fresh copy so callers can mutate it without affecting the cache.
    """
    path = CONFIG_DIR / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    return copy.deepcopy(_load_yaml(path))


def dataset_config(dataset: str) -> dict[str, Any]:
    """Return the ``configs/data.yaml`` section for one dataset."""
    data_cfg = load_config("data")
    datasets = data_cfg.get("datasets", {})
    if dataset not in datasets:
        raise ValueError(f"Unknown dataset '{dataset}'. Known: {sorted(datasets)}")
    return datasets[dataset]


@dataclass(frozen=True)
class AppConfig:
    """Service limits from ``configs/app.yaml``."""

    max_body_bytes: int
    max_inflight_requests: int
    worker_threads: int
    rate_limit_per_second: float
    rate_limit_burst: int
    page_size_default: int
    page_size_max: int
    db_pool_size: int
    db_max_overflow: int
    db_pool_timeout_s: float
    db_statement_timeout_ms: int
    db_lock_timeout_ms: int
    db_connect_timeout_s: int
    simulation_epoch: datetime
    require_approved_policy: bool

    @classmethod
    def load(cls, **overrides: Any) -> AppConfig:
        c = load_config("app")
        epoch = datetime.fromisoformat(c["simulation"]["epoch"].replace("Z", "+00:00"))
        if epoch.tzinfo is None:
            raise ValueError("simulation.epoch must include a timezone")
        values = dict(
            max_body_bytes=int(c["api"]["max_body_bytes"]),
            max_inflight_requests=int(c["api"]["max_inflight_requests"]),
            worker_threads=int(c["api"]["worker_threads"]),
            rate_limit_per_second=float(c["api"]["rate_limit"]["per_second"]),
            rate_limit_burst=int(c["api"]["rate_limit"]["burst"]),
            page_size_default=int(c["api"]["pagination"]["default_limit"]),
            page_size_max=int(c["api"]["pagination"]["max_limit"]),
            db_pool_size=int(c["database"]["pool_size"]),
            db_max_overflow=int(c["database"]["max_overflow"]),
            db_pool_timeout_s=float(c["database"]["pool_timeout_seconds"]),
            db_statement_timeout_ms=int(c["database"]["statement_timeout_ms"]),
            db_lock_timeout_ms=int(c["database"]["lock_timeout_ms"]),
            db_connect_timeout_s=int(c["database"]["connect_timeout_seconds"]),
            simulation_epoch=epoch.astimezone(UTC),
            require_approved_policy=bool(c["model"]["require_approved_policy"]),
        )
        values.update(overrides)
        return cls(**values)


def ensure_directories() -> None:
    """Create the working directories used by the pipelines."""
    for rel in [
        "data/raw",
        "data/interim",
        "data/processed",
        "data/synthetic",
        "logs",
        "reports/eda",
        "reports/model_evaluation",
        "models",
    ]:
        (PROJECT_ROOT / rel).mkdir(parents=True, exist_ok=True)
