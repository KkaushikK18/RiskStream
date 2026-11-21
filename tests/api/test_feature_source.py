"""Scoring with Redis features: source recorded, fallback to PostgreSQL, readiness degraded."""

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from riskstream.config import StreamingConfig
from riskstream.features.derive import primitive_columns
from riskstream.features.online_store import FeatureStoreError, FeatureStoreNotUsable
from riskstream.features.registry import get_registry
from riskstream.storage.models import PredictionRow
from tests.api.conftest import auth, txn

WINDOWS = get_registry().config.windows


class StubStore:
    def __init__(self, behaviour="ok", meta=None):
        self.behaviour, self.meta = behaviour, meta or {}
        self.calls = 0

    def primitives(self, dest_id, orig_id, step, check_freshness=True):
        self.calls += 1
        if self.behaviour == "down":
            raise FeatureStoreError("connection refused")
        if self.behaviour == "stale":
            raise FeatureStoreNotUsable("consumer_heartbeat_stale")
        return {c: (None if c.endswith("last_step") else 0.0) for c in primitive_columns(WINDOWS)}

    def read_meta(self):
        if self.behaviour == "down":
            raise FeatureStoreError("down") from ConnectionError()
        return self.meta


def stored(db_url):
    with Session(create_engine(db_url)) as s:
        p = s.execute(select(PredictionRow)).scalar_one()
        return p.feature_source, p.feature_fallback_reason


@pytest.fixture
def redis_client(make_client):
    def _make(store):
        return make_client(stream_cfg=StreamingConfig.load(serving_feature_source="redis"), feature_store=store)
    return _make


def test_redis_features_used_and_recorded(redis_client, db_url):
    store = StubStore("ok")
    c = redis_client(store)
    r = c.post("/v1/score", json=txn(), headers=auth())
    assert r.status_code == 201 and r.json()["feature_source"] == "redis"
    assert stored(db_url) == ("redis", None) and store.calls == 1


@pytest.mark.parametrize(("behaviour", "reason"), [("down", "redis_unavailable"),
                                                   ("stale", "consumer_heartbeat_stale")])
def test_fallback_to_postgres(redis_client, db_url, behaviour, reason):
    c = redis_client(StubStore(behaviour))
    r = c.post("/v1/score", json=txn(), headers=auth())
    assert r.status_code == 201 and r.json()["feature_source"] == "postgres_fallback"
    assert stored(db_url) == ("postgres_fallback", reason)
    assert f'riskstream_feature_fallbacks_total{{reason="{reason}"}}' in c.get("/metrics").text


def test_readiness_degraded_not_unready_when_redis_down(redis_client):
    r = redis_client(StubStore("down")).get("/health/ready")
    body = r.json()
    assert r.status_code == 200 and body["status"] == "degraded"
    dep = {d["name"]: d for d in body["dependencies"]}["online_features"]
    assert dep["essential"] is False and dep["status"] == "down"


def test_readiness_degraded_when_pipeline_stale(redis_client):
    body = redis_client(StubStore("ok", meta={})).get("/health/ready").json()
    dep = {d["name"]: d for d in body["dependencies"]}["online_features"]
    assert body["status"] == "degraded" and "not_bootstrapped" in dep["detail"]


def test_replay_reports_original_source(redis_client):
    c = redis_client(StubStore("ok"))
    c.post("/v1/score", json=txn(), headers=auth())
    again = c.post("/v1/score", json=txn(), headers=auth())
    assert again.status_code == 200 and again.json()["feature_source"] == "redis"
