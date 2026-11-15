"""Configuration management for RiskStream."""

import os
from pathlib import Path
from typing import Any, Dict

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Environment
    environment: str = Field(default="development", env="ENVIRONMENT")
    random_seed: int = Field(default=42, env="RANDOM_SEED")

    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(default="json", env="LOG_FORMAT")

    # Data paths
    data_raw_dir: Path = Field(default=Path("data/raw"), env="DATA_RAW_DIR")
    data_interim_dir: Path = Field(default=Path("data/interim"), env="DATA_INTERIM_DIR")
    data_processed_dir: Path = Field(default=Path("data/processed"), env="DATA_PROCESSED_DIR")
    data_synthetic_dir: Path = Field(default=Path("data/synthetic"), env="DATA_SYNTHETIC_DIR")

    # PaySim dataset
    paysim_path: Path = Field(
        default=Path("data/raw/paysim/PS_20174392719_1491204439457_log.csv"),
        env="PAYSIM_PATH",
    )

    # Chronological split
    train_end_step: int = Field(default=595, env="TRAIN_END_STEP")
    val_end_step: int = Field(default=669, env="VAL_END_STEP")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


def load_config(config_name: str) -> Dict[str, Any]:
    """
    Load a YAML configuration file from the configs/ directory.

    Args:
        config_name: Name of the config file (without .yaml extension)

    Returns:
        Dictionary containing configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
    """
    config_path = Path("configs") / f"{config_name}.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config


def get_project_root() -> Path:
    """
    Get the project root directory.

    Returns:
        Path to project root
    """
    # Assume this file is in src/riskstream/
    return Path(__file__).parent.parent.parent


def ensure_directories() -> None:
    """Create necessary directories if they don't exist."""
    settings = Settings()
    directories = [
        settings.data_raw_dir,
        settings.data_interim_dir,
        settings.data_processed_dir,
        settings.data_synthetic_dir,
        Path("logs"),
        Path("reports/eda"),
        Path("models"),
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


# Global settings instance
settings = Settings()
