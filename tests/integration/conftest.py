"""Before integration tests run, remove what killed earlier runs left behind
(see tests/integration/leftovers.py for the safety rules)."""

from __future__ import annotations

import os
import warnings

import pytest

from tests.integration import leftovers


@pytest.fixture(scope="session", autouse=True)
def clean_leftovers_from_killed_runs():
    kafka = os.environ.get("RISKSTREAM_TEST_KAFKA")
    redis_url = os.environ.get("RISKSTREAM_TEST_REDIS_URL")
    # Cleanup problems must not hide the real test results: warn and continue.
    if kafka:
        try:
            removed = leftovers.delete_stale_topics(kafka)
            if removed:
                warnings.warn(f"deleted {len(removed)} leftover test topic(s) from killed runs", stacklevel=1)
        except Exception as exc:
            warnings.warn(f"leftover topic cleanup failed: {exc}", stacklevel=1)
    if redis_url:
        try:
            n = leftovers.delete_stale_keys(redis_url)
            if n:
                warnings.warn(f"deleted {n} leftover test Redis key(s) from killed runs", stacklevel=1)
        except Exception as exc:
            warnings.warn(f"leftover Redis cleanup failed: {exc}", stacklevel=1)
    yield
