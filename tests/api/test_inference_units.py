import json

import pytest

from riskstream.api.security import ApiKeyAuthenticator, RateLimiter, hash_key
from riskstream.config import ApiKeyEntry
from riskstream.features.registry import get_registry
from riskstream.inference.decision_policy import DecisionPolicy, PolicyError
from riskstream.inference.model_loader import ModelLoadError, load_serving_model
from riskstream.inference.predictor import ServiceError, timestamp_to_step
from tests.api.conftest import EPOCH


def _policy(tmp_path, **kw):
    data = {"policy_version": "p1", "model_version": "m1", "thresholds": {"review": 0.1, "decline": 0.5},
            "approval": {"status": "unapproved"}} | kw
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(data))
    return path


def test_policy_decisions(tmp_path):
    p = DecisionPolicy.load(_policy(tmp_path))
    assert p.decide(0.05).decision == "APPROVE"
    assert p.decide(0.1).decision == "REVIEW" and p.decide(0.1).risk_level == "MEDIUM"
    assert p.decide(0.5).decision == "DECLINE"
    with pytest.raises(ValueError):
        p.decide(1.5)


def test_never_threshold_above_one(tmp_path):
    p = DecisionPolicy.load(_policy(tmp_path, thresholds={"review": 1.0000000000000002,
                                                          "decline": 1.0000000000000002}))
    assert p.decide(1.0).decision == "APPROVE"


@pytest.mark.parametrize("thresholds", [{"review": 0.6, "decline": 0.5}, {"review": -1, "decline": 0.5},
                                        {"review": 0.1}])
def test_invalid_policies_rejected(tmp_path, thresholds):
    with pytest.raises(PolicyError):
        DecisionPolicy.load(_policy(tmp_path, thresholds=thresholds))


def test_loader_rejects_mismatched_policy(artifacts, tmp_path):
    with pytest.raises(ModelLoadError, match="tuned for"):
        load_serving_model(artifacts["bundle"], _policy(tmp_path), get_registry(), False)


def test_loader_requires_approval_when_configured(artifacts):
    with pytest.raises(ModelLoadError, match="not approved"):
        load_serving_model(artifacts["bundle"], artifacts["policy"], get_registry(), True)


def test_loader_rejects_feature_schema_change(artifacts, tmp_path):
    meta_path = artifacts["bundle"] / "metadata.json"
    original = meta_path.read_text()
    try:
        meta_path.write_text(json.dumps(json.loads(original) | {"feature_schema_hash": "0" * 16}))
        with pytest.raises(ModelLoadError, match="feature schema"):
            load_serving_model(artifacts["bundle"], artifacts["policy"], get_registry(), False)
    finally:
        meta_path.write_text(original)


def test_loader_requires_explicit_paths(artifacts):
    with pytest.raises(ModelLoadError, match="must both be set"):
        load_serving_model(None, artifacts["policy"], get_registry(), False)


def test_loaded_model_is_single_threaded(artifacts):
    m = load_serving_model(artifacts["bundle"], artifacts["policy"], get_registry(), False)
    assert m.bundle.model.get_params()["n_jobs"] == 1


def test_timestamp_to_step():
    from datetime import timedelta

    assert timestamp_to_step(EPOCH, EPOCH) == 1
    assert timestamp_to_step(EPOCH + timedelta(minutes=59), EPOCH) == 1
    assert timestamp_to_step(EPOCH + timedelta(hours=1), EPOCH) == 2
    with pytest.raises(ServiceError):
        timestamp_to_step(EPOCH - timedelta(seconds=1), EPOCH)


def test_authenticator_and_roles():
    auth = ApiKeyAuthenticator([ApiKeyEntry(name="a", roles=["reader"], sha256=hash_key("k"))])
    assert auth.authenticate("k", "reader").name == "a"
    with pytest.raises(Exception, match="lacks"):
        auth.authenticate("k", "scorer")
    with pytest.raises(ValueError, match="unknown roles"):
        ApiKeyAuthenticator([ApiKeyEntry(name="b", roles=["admin"], sha256=hash_key("k"))])


def test_rate_limiter_refills(monkeypatch):
    import riskstream.api.security as sec

    now = [100.0]
    monkeypatch.setattr(sec.time, "monotonic", lambda: now[0])
    limiter = RateLimiter(per_second=2.0, burst=2)
    principal = sec.Principal("c", frozenset({"scorer"}), None)
    limiter.check(principal)
    limiter.check(principal)
    with pytest.raises(Exception, match="Rate limit"):
        limiter.check(principal)
    now[0] += 0.5  # one token back at 2/s
    limiter.check(principal)
