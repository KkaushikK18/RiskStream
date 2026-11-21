"""API test fixtures: a migrated SQLite database and an app wired to the test bundle."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from riskstream.api.main import create_app
from riskstream.api.security import hash_key
from riskstream.config import ApiKeyEntry, AppConfig, Settings
from riskstream.storage.admin import upgrade
from tests.conftest import DECLINE_T, EPOCH, MODEL_VERSION, REVIEW_T, ts, txn

KEYS = {"scorer": "test-scorer-key", "reader": "test-reader-key", "labeler": "test-labeler-key",
        "all": "test-all-key"}


def make_settings(artifacts, db_url, **overrides) -> Settings:
    entries = [
        ApiKeyEntry(name="scorer-svc", roles=["scorer"], sha256=hash_key(KEYS["scorer"])),
        ApiKeyEntry(name="reader-svc", roles=["reader"], sha256=hash_key(KEYS["reader"])),
        ApiKeyEntry(name="labeler-svc", roles=["labeler"], sha256=hash_key(KEYS["labeler"])),
        ApiKeyEntry(name="ops", roles=["scorer", "reader", "labeler"], sha256=hash_key(KEYS["all"])),
    ]
    values = dict(_env_file=None, database_url=db_url, model_bundle_path=artifacts["bundle"],
                  policy_path=artifacts["policy"], api_keys=entries, instance_id="test-instance")
    values.update(overrides)
    return Settings(**values)


@pytest.fixture
def db_url(tmp_path):
    url = f"sqlite:///{(tmp_path / 'api.db').as_posix()}"
    upgrade(url)
    return url


@pytest.fixture
def make_client(artifacts, db_url):
    clients = []

    def _make(cfg_overrides: dict | None = None, **settings_overrides):
        settings = make_settings(artifacts, db_url, **settings_overrides)
        cfg = AppConfig.load(**(cfg_overrides or {}))
        engine = create_engine(db_url, connect_args={"check_same_thread": False})
        app = create_app(settings, cfg, engine=engine)
        client = TestClient(app, raise_server_exceptions=False)
        client.__enter__()
        clients.append((client, engine))
        return client

    yield _make
    for client, engine in clients:
        client.__exit__(None, None, None)
        engine.dispose()


@pytest.fixture
def client(make_client):
    return make_client()


def auth(role: str = "all") -> dict[str, str]:
    return {"X-API-Key": KEYS[role]}


__all__ = ["auth", "txn", "ts", "KEYS", "MODEL_VERSION", "REVIEW_T", "DECLINE_T", "EPOCH"]
