"""Configuration management.

Two sources of configuration, each with one job:

* Environment variables (``.env``) hold deployment-level settings that change
  between machines: log level, random seed, tracking URI.
* YAML files in ``configs/`` hold versioned, reviewable pipeline settings:
  dataset paths, split boundaries, feature definitions, model search spaces.

All relative paths are resolved against the project root, so commands behave
the same regardless of the current working directory.
"""

from __future__ import annotations

from functools import cache, lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
CONFIG_DIR: Path = PROJECT_ROOT / "configs"


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
    import copy

    return copy.deepcopy(_load_yaml(path))


def dataset_config(dataset: str) -> dict[str, Any]:
    """Return the ``configs/data.yaml`` section for one dataset."""
    data_cfg = load_config("data")
    datasets = data_cfg.get("datasets", {})
    if dataset not in datasets:
        raise ValueError(f"Unknown dataset '{dataset}'. Known: {sorted(datasets)}")
    return datasets[dataset]


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
