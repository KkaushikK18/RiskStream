import logging
import math

import polars as pl
import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from riskstream.features.offline import build_feature_frame
from riskstream.features.registry import get_registry
from riskstream.storage.models import (
    AuditEventRow,
    ModelDeploymentRow,
    OutboxEventRow,
    PredictionRow,
    TransactionRow,
)
from tests.api.conftest import DECLINE_T, MODEL_VERSION, REVIEW_T, auth, ts, txn
from tests.conftest import raw_rows, to_canonical


def count(db_url, model) -> int:
    with Session(create_engine(db_url)) as s:
        return s.execute(select(func.count()).select_from(model)).scalar_one()


# ------------------------------------------------------------------ health


def test_liveness(client):
    r = client.get("/health/live")
    assert r.status_code == 200 and r.json()["status"] == "alive"


def test_readiness_ready(client):
    r = client.get("/health/ready")
    body = r.json()
    assert r.status_code == 200 and body["status"] == "ready"
    assert body["model_version"] == MODEL_VERSION
    assert {d["name"]: d["status"] for d in body["dependencies"]} == {"model": "ok", "database": "ok"}


def test_missing_model_not_ready_and_scoring_unavailable(make_client, artifacts):
    c = make_client(model_bundle_path=artifacts["dir"] / "does-not-exist")
    assert c.get("/health/live").status_code == 200
    r = c.get("/health/ready")
    assert r.status_code == 503 and r.json()["status"] == "not_ready"
    s = c.post("/v1/score", json=txn(), headers=auth())
    assert s.status_code == 503 and s.json()["error"]["code"] == "service_unavailable"


def test_database_down_not_ready(client, monkeypatch):
    import riskstream.api.routes.health as health

    def boom(_):
        raise OperationalError("SELECT 1", {}, Exception("connection refused"))

    monkeypatch.setattr(health, "ping", boom)
    r = client.get("/health/ready")
    assert r.status_code == 503
    assert {d["name"]: d["status"] for d in r.json()["dependencies"]}["database"] == "down"


def test_deployment_recorded_once(make_client, db_url):
    make_client()
    make_client()  # second instance start with the same model
    with Session(create_engine(db_url)) as s:
        deployments = s.execute(select(ModelDeploymentRow)).scalars().all()
        loads = s.execute(select(func.count()).select_from(AuditEventRow)
                          .where(AuditEventRow.event_type == "model_loaded")).scalar_one()
    assert len(deployments) == 1 and deployments[0].status == "active"
    assert loads == 2


# ------------------------------------------------------------------ scoring


def test_score_returns_prediction(client, db_url):
    r = client.post("/v1/score", json=txn(), headers=auth("scorer"))
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["transaction_id"] == "t1" and body["model_version"] == MODEL_VERSION
    assert 0.0 <= body["fraud_probability"] <= 1.0
    p = body["fraud_probability"]
    expected = "DECLINE" if p >= DECLINE_T else "REVIEW" if p >= REVIEW_T else "APPROVE"
    assert body["decision"] == expected
    assert body["risk_level"] == {"APPROVE": "LOW", "REVIEW": "MEDIUM", "DECLINE": "HIGH"}[expected]
    assert body["idempotent_replay"] is False
    assert set(body["latency"]) == {"feature_ms", "inference_ms", "total_ms"}
    assert r.headers["location"] == "/v1/transactions/t1"
    assert r.headers["x-request-id"]
    assert count(db_url, TransactionRow) == 1 and count(db_url, PredictionRow) == 1
    assert count(db_url, OutboxEventRow) == 1


def test_request_and_correlation_ids_propagate(client):
    r = client.post("/v1/score", json=txn(), headers=auth() | {"X-Request-ID": "req-123", "X-Correlation-ID": "c-9"})
    assert r.headers["x-request-id"] == "req-123" and r.headers["x-correlation-id"] == "c-9"
    bad = client.get("/health/live", headers={"X-Request-ID": "has spaces & symbols"})
    assert bad.headers["x-request-id"] != "has spaces & symbols"


def test_identical_retry_is_idempotent(client, db_url):
    first = client.post("/v1/score", json=txn(), headers=auth())
    second = client.post("/v1/score", json=txn(), headers=auth())
    assert second.status_code == 200
    assert second.json()["idempotent_replay"] is True
    assert second.json()["fraud_probability"] == first.json()["fraud_probability"]
    assert count(db_url, PredictionRow) == 1 and count(db_url, OutboxEventRow) == 1


def test_same_id_different_content_conflicts(client, db_url):
    client.post("/v1/score", json=txn(), headers=auth())
    r = client.post("/v1/score", json=txn(amount=99999.0), headers=auth())
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "transaction_id_conflict"
    assert count(db_url, TransactionRow) == 1


def test_concurrent_duplicate_returns_stored_result(client, db_url, monkeypatch):
    """Another request commits the same transaction between our check and our insert."""
    import riskstream.inference.predictor as predictor

    service = client.app.state.service
    real = predictor.history_primitives
    calls = {"n": 0}

    def racing(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            from riskstream.api.schemas.scoring import ScoreRequest

            service.score(ScoreRequest(**txn()), "other-instance")  # wins the race
        return real(*args, **kwargs)

    monkeypatch.setattr(predictor, "history_primitives", racing)
    r = client.post("/v1/score", json=txn(), headers=auth())
    assert r.status_code == 200 and r.json()["idempotent_replay"] is True
    assert count(db_url, TransactionRow) == 1 and count(db_url, PredictionRow) == 1


@pytest.mark.parametrize("change", [
    {"amount": -1.0}, {"amount": "lots"}, {"type": "WIRE"}, {"dest_type": "BANK"},
    {"timestamp": "2026-01-02T10:00:00"},  # no timezone
    {"transaction_id": "bad id with spaces"}, {"unexpected": 1}, {"orig_balance_before": float("inf")},
])
def test_invalid_requests_rejected(client, change, db_url):
    body = txn() | change
    r = client.post("/v1/score", content=__import__("json").dumps(body, allow_nan=True),
                    headers=auth() | {"content-type": "application/json"})
    assert r.status_code == 422, r.text
    err = r.json()["error"]
    assert err["code"] == "validation_error" and err["details"]
    assert "lots" not in r.text  # submitted values are not echoed
    assert count(db_url, TransactionRow) == 0


def test_missing_field_rejected(client):
    body = txn()
    del body["amount"]
    assert client.post("/v1/score", json=body, headers=auth()).status_code == 422


def test_timestamp_before_epoch(client):
    r = client.post("/v1/score", json=txn(timestamp="2025-12-31T23:00:00Z"), headers=auth())
    assert r.status_code == 422 and r.json()["error"]["code"] == "timestamp_before_epoch"


def test_history_feeds_later_features_with_offline_semantics(client, db_url, tmp_path):
    """Score a small event stream via HTTP; stored features equal the offline pipeline's."""
    events = [
        dict(transaction_id="h1", step=1, dest_account_id="C777", amount=100.0, type="TRANSFER"),
        dict(transaction_id="h2", step=1, dest_account_id="C777", amount=300.0, type="CASH_OUT", orig_account_id="C5"),
        dict(transaction_id="h3", step=3, dest_account_id="C777", amount=5000.0, type="TRANSFER", orig_account_id="C6"),
        dict(transaction_id="h4", step=3, dest_account_id="C777", amount=10.0, type="TRANSFER", orig_account_id="C7"),
        dict(transaction_id="h5", step=40, dest_account_id="C777", amount=42.0, type="TRANSFER", orig_account_id="C1000001"),
    ]
    for e in events:
        body = txn(e["transaction_id"], e["step"], **{k: v for k, v in e.items() if k not in ("transaction_id", "step")})
        body["orig_balance_before"] = body["amount"]
        assert client.post("/v1/score", json=body, headers=auth()).status_code == 201

    raw = raw_rows([{"step": e["step"], "type": e["type"], "amount": e["amount"],
                     "nameOrig": e.get("orig_account_id", "C1000001"), "oldbalanceOrg": e["amount"],
                     "newbalanceOrig": 0.0, "nameDest": e["dest_account_id"]} for e in events])
    registry = get_registry()
    offline = build_feature_frame(to_canonical(raw, tmp_path), registry).collect().sort("event_seq")
    with Session(create_engine(db_url)) as s:
        stored = {p.transaction_id: p.features for p in s.execute(select(PredictionRow)).scalars()}
    for i, e in enumerate(events):
        ref = offline.row(i, named=True)
        for name in registry.names("full"):
            got = stored[e["transaction_id"]][name]
            want = ref[name]
            if want is None or math.isnan(want):
                assert got is None, (e["transaction_id"], name, got)
            else:
                assert got == pytest.approx(want, rel=1e-5, abs=1e-5), (e["transaction_id"], name)
    assert stored["h3"]["dest_txn_count_24h"] == 2  # both step-1 events, not the same-step h4
    assert stored["h5"]["dest_txn_count_24h"] == 0 and stored["h5"]["dest_txn_count_all"] == 4


def test_database_failure_fails_closed(client, db_url, monkeypatch):
    import riskstream.inference.predictor as predictor

    def down(*a, **k):
        raise OperationalError("SELECT", {}, Exception("server closed the connection"))

    monkeypatch.setattr(predictor, "history_primitives", down)
    r = client.post("/v1/score", json=txn(), headers=auth())
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "dependency_unavailable"
    assert r.json()["error"]["request_id"] == r.headers["x-request-id"]
    assert r.headers["retry-after"]
    assert "decision" not in r.json() and "fraud_probability" not in r.json()
    assert count(db_url, TransactionRow) == 0


def test_outbox_failure_rolls_back_everything(client, db_url, monkeypatch):
    import riskstream.inference.predictor as predictor

    def broken(*a, **k):
        raise RuntimeError("outbox write failed")

    monkeypatch.setattr(predictor, "add_outbox_event", broken)
    r = client.post("/v1/score", json=txn(), headers=auth())
    assert r.status_code == 500
    assert r.json()["error"] == {"code": "internal_error", "message": "Internal server error",
                                 "request_id": r.headers["x-request-id"], "details": None}
    assert count(db_url, TransactionRow) == 0 and count(db_url, PredictionRow) == 0


# ------------------------------------------------------------------ auth, limits


def test_auth_required(client):
    assert client.post("/v1/score", json=txn()).status_code == 401
    r = client.post("/v1/score", json=txn(), headers={"X-API-Key": "wrong"})
    assert r.status_code == 401 and r.json()["error"]["code"] == "unauthorized"


def test_role_enforced(client):
    r = client.post("/v1/score", json=txn(), headers=auth("reader"))
    assert r.status_code == 403 and r.json()["error"]["code"] == "forbidden"
    assert client.get("/v1/transactions/t1", headers=auth("scorer")).status_code == 403
    assert client.post("/v1/labels", json={}, headers=auth("reader")).status_code == 403


def test_rate_limit(make_client):
    c = make_client({"rate_limit_per_second": 1.0, "rate_limit_burst": 2})
    codes = [c.post("/v1/score", json=txn(f"r{i}"), headers=auth()).status_code for i in range(4)]
    assert codes[:2] == [201, 201] and codes[2] == 429
    r = c.post("/v1/score", json=txn("r9"), headers=auth())
    assert r.status_code == 429 and int(r.headers["retry-after"]) >= 1
    assert r.json()["error"]["code"] == "rate_limited"


def test_body_size_limit(make_client):
    c = make_client({"max_body_bytes": 300})
    big = txn() | {"transaction_id": "x" * 64}
    r = c.post("/v1/score", content=b"{" + b" " * 400 + b"}", headers=auth() | {"content-type": "application/json"})
    assert r.status_code == 413 and r.json()["error"]["code"] == "payload_too_large"
    assert c.post("/v1/score", json=big, headers=auth()).status_code in (201, 413)


def test_body_size_limit_without_content_length(make_client):
    c = make_client({"max_body_bytes": 300})

    def chunks():
        yield b'{"transaction_id": "'
        yield b"a" * 500
        yield b'"}'

    r = c.post("/v1/score", content=chunks(), headers=auth() | {"content-type": "application/json"})
    assert r.status_code == 413


def test_load_shedding(make_client):
    c = make_client({"max_inflight_requests": 0})
    r = c.post("/v1/score", json=txn(), headers=auth())
    assert r.status_code == 503 and r.json()["error"]["code"] == "overloaded"
    assert c.get("/health/live").status_code == 200


# ------------------------------------------------------------------ reads


def test_get_transaction(client):
    client.post("/v1/score", json=txn(), headers=auth())
    r = client.get("/v1/transactions/t1", headers=auth("reader"))
    assert r.status_code == 200
    body = r.json()
    assert body["transaction"]["orig_account"] == "C***0001"
    assert "C1000001" not in r.text and "C2000002" not in r.text
    assert body["prediction"]["model_version"] == MODEL_VERSION and body["label"] is None
    assert client.get("/v1/transactions/nope", headers=auth("reader")).status_code == 404


def test_prediction_pagination(client):
    for i in range(5):
        client.post("/v1/score", json=txn(f"p{i}", step=10 + i), headers=auth())
    seen, cursor = [], None
    while True:
        params = {"limit": 2} | ({"cursor": cursor} if cursor else {})
        page = client.get("/v1/predictions", params=params, headers=auth("reader")).json()
        seen += [i["transaction_id"] for i in page["items"]]
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert seen == ["p4", "p3", "p2", "p1", "p0"]
    assert client.get("/v1/predictions", params={"cursor": "!!"}, headers=auth("reader")).status_code == 422
    assert client.get("/v1/predictions", params={"limit": 0}, headers=auth("reader")).status_code == 422


def test_model_info(client):
    r = client.get("/v1/model", headers=auth("reader"))
    assert r.status_code == 200
    assert r.json()["review_threshold"] == REVIEW_T and r.json()["is_synthetic_training_data"] is True


# ------------------------------------------------------------------ labels


def label(transaction_id="t1", is_fraud=True, step=12, source="chargeback") -> dict:
    return {"transaction_id": transaction_id, "is_fraud": is_fraud, "source": source, "label_time": ts(step)}


def test_label_lifecycle(client, db_url):
    client.post("/v1/score", json=txn(), headers=auth())
    assert client.post("/v1/labels", json=label("missing"), headers=auth("labeler")).status_code == 404
    first = client.post("/v1/labels", json=label(), headers=auth("labeler"))
    assert first.status_code == 201 and first.json()["idempotent_replay"] is False
    again = client.post("/v1/labels", json=label(), headers=auth("labeler"))
    assert again.status_code == 200 and again.json()["idempotent_replay"] is True
    conflict = client.post("/v1/labels", json=label(is_fraud=False), headers=auth("labeler"))
    assert conflict.status_code == 409 and conflict.json()["error"]["code"] == "label_conflict"
    detail = client.get("/v1/transactions/t1", headers=auth("reader")).json()
    assert detail["label"]["is_fraud"] is True
    with Session(create_engine(db_url)) as s:
        topics = [o.topic for o in s.execute(select(OutboxEventRow)).scalars()]
        audits = s.execute(select(func.count()).select_from(AuditEventRow)
                           .where(AuditEventRow.event_type == "label_recorded")).scalar_one()
    assert topics.count("transactions.labeled") == 1 and audits == 1


def test_label_time_validation(client):
    client.post("/v1/score", json=txn(step=10), headers=auth())
    early = client.post("/v1/labels", json=label(step=9), headers=auth("labeler"))
    assert early.status_code == 422 and early.json()["error"]["code"] == "label_before_transaction"
    future = client.post("/v1/labels", json=label() | {"label_time": "2099-01-01T00:00:00Z"},
                         headers=auth("labeler"))
    assert future.status_code == 422 and future.json()["error"]["code"] == "label_in_future"


# ------------------------------------------------------------------ observability


def test_metrics_use_route_templates(client):
    client.post("/v1/score", json=txn(), headers=auth())
    client.get("/v1/transactions/t1", headers=auth("reader"))
    text_ = client.get("/metrics").text
    assert 'route="/v1/transactions/{transaction_id}"' in text_
    assert "/v1/transactions/t1" not in text_
    assert "riskstream_score_stage_seconds" in text_ and "riskstream_decisions_total" in text_
    assert "riskstream_model_loaded 1.0" in text_


def test_logs_do_not_contain_payload(client):
    records: list[str] = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(self.format(record) + " " + str(record.__dict__))

    h = Capture()
    root = logging.getLogger("riskstream")
    old_level = root.level
    root.setLevel(logging.DEBUG)  # what setup_logging() configures in production
    root.addHandler(h)
    try:
        client.post("/v1/score", json=txn(amount=1234.56, orig_balance_before=7777.77), headers=auth())
    finally:
        root.removeHandler(h)
        root.setLevel(old_level)
    joined = "\n".join(records)
    assert "transaction_scored" in joined
    for secret in ("1234.56", "7777.77", "C1000001", "C2000002", "test-all-key"):
        assert secret not in joined


def test_openapi_documents_security_and_endpoints(client):
    spec = client.get("/openapi.json").json()
    assert {"/v1/score", "/v1/labels", "/v1/transactions/{transaction_id}", "/v1/predictions",
            "/health/live", "/health/ready"} <= set(spec["paths"])
    assert "APIKeyHeader" in spec["components"]["securitySchemes"]


def test_no_rows_written_for_unauthenticated(client, db_url):
    client.post("/v1/score", json=txn())
    assert count(db_url, TransactionRow) == 0
    with create_engine(db_url).connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM audit_events WHERE event_type='label_recorded'")).scalar_one() == 0


__all__ = ["pl"]
