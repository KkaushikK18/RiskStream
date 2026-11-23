"""Phase 6 gauges on /metrics: online-store freshness verdict and DB pool capacity."""

from __future__ import annotations

import re
import time

import pytest

from riskstream.api.routes.health import FRESHNESS_REASONS
from riskstream.config import StreamingConfig
from riskstream.features.online_store import FeatureStoreError


def _sample(text: str, name: str, labels: str = "") -> float | None:
    m = re.search(rf"^{re.escape(name)}{re.escape(labels)} (\S+)$", text, re.M)
    return float(m.group(1)) if m else None


class MetaStore:
    def __init__(self, meta=None, down=False):
        self.meta, self.down = meta or {}, down

    def read_meta(self):
        if self.down:
            raise FeatureStoreError("down") from ConnectionError()
        return self.meta


def _healthy_meta(cfg) -> dict:
    now = str(int(time.time() * 1000))
    return {"bootstrap_completed_at_ms": now, "layout": cfg.namespace, "relay_heartbeat_ms": now,
            "consumer_heartbeat_ms": now, "consumer_state": "running"}


def _active(text: str) -> list[str]:
    return [r for r in FRESHNESS_REASONS
            if _sample(text, "riskstream_online_store_state", f'{{reason="{r}"}}') == 1.0]


@pytest.mark.parametrize("case", ["ok", "not_bootstrapped", "consumer_heartbeat_stale", "redis_unavailable"])
def test_online_store_gauges_follow_freshness_verdict(make_client, case):
    cfg = StreamingConfig.load(serving_feature_source="redis")
    meta = _healthy_meta(cfg)
    store = {
        "ok": MetaStore(meta),
        "not_bootstrapped": MetaStore({}),
        "consumer_heartbeat_stale": MetaStore({**meta, "consumer_heartbeat_ms": "1"}),
        "redis_unavailable": MetaStore(down=True),
    }[case]
    text = make_client(stream_cfg=cfg, feature_store=store).get("/metrics").text
    assert _sample(text, "riskstream_online_store_usable") == (1.0 if case == "ok" else 0.0)
    assert _active(text) == [case]  # exactly one reason is active; the rest are 0


def test_online_store_check_error_counted(make_client):
    cfg = StreamingConfig.load(serving_feature_source="redis")
    c = make_client(stream_cfg=cfg, feature_store=MetaStore(down=True))
    before = _sample(c.get("/metrics").text, "riskstream_online_store_check_errors_total")
    after = _sample(c.get("/metrics").text, "riskstream_online_store_check_errors_total")
    assert before is not None and after == before + 1


def test_online_store_gauges_switch_back_when_store_recovers(make_client):
    cfg = StreamingConfig.load(serving_feature_source="redis")
    store = MetaStore(down=True)
    c = make_client(stream_cfg=cfg, feature_store=store)
    assert _active(c.get("/metrics").text) == ["redis_unavailable"]
    store.down, store.meta = False, _healthy_meta(cfg)
    text = c.get("/metrics").text
    assert _active(text) == ["ok"] and _sample(text, "riskstream_online_store_usable") == 1.0


def test_not_configured_when_redis_url_missing(make_client):
    text = make_client(stream_cfg=StreamingConfig.load(serving_feature_source="redis")).get("/metrics").text
    assert _active(text) == ["not_configured"]


def test_db_pool_gauges(client):
    text = client.get("/metrics").text
    cap = _sample(text, "riskstream_db_pool_capacity")
    size = _sample(text, "riskstream_db_pool_size")
    assert cap is not None and size is not None and cap >= size
