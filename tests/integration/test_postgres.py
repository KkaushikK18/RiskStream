"""PostgreSQL integration tests. Run with a disposable database:

    $env:RISKSTREAM_TEST_DATABASE_URL = "postgresql+psycopg://riskstream:...@localhost:5433/riskstream_test"
    python -m pytest tests/integration -q

The tests drop and recreate every RiskStream table in that database.
"""

from __future__ import annotations

import concurrent.futures
import os
import threading

import pytest

URL = os.environ.get("RISKSTREAM_TEST_DATABASE_URL")
pytestmark = [pytest.mark.postgres, pytest.mark.skipif(not URL, reason="RISKSTREAM_TEST_DATABASE_URL not set")]


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    from riskstream.storage.admin import alembic_config, upgrade

    engine = create_engine(URL)
    from alembic import command

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
        for t in ("audit_events", "outbox_events", "model_evaluations", "model_deployments", "labels",
                  "predictions", "transactions"):
            conn.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE"))
    upgrade(URL)
    yield engine
    command.downgrade(alembic_config(URL), "base")
    engine.dispose()


def test_schema_matches_models(pg_engine):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    from riskstream.storage.models import Base

    with pg_engine.connect() as conn:
        assert compare_metadata(MigrationContext.configure(conn), Base.metadata) == []


def test_copy_backfill_and_sql_parity(pg_engine, small_canonical):
    import math

    import polars as pl

    from riskstream.features.derive import primitive_columns
    from riskstream.features.offline import compute_primitives
    from riskstream.storage.admin import seed_history
    from riskstream.storage.database import session_factory
    from riskstream.storage.repositories.feature_history import history_primitives

    first = seed_history(pg_engine, "synthetic", canonical=small_canonical)
    again = seed_history(pg_engine, "synthetic", canonical=small_canonical)
    assert first["rows_inserted"] == small_canonical.select(pl.len()).collect().item()
    assert again["rows_inserted"] == 0
    offline = compute_primitives(small_canonical, (24, 168)).collect().sample(300, seed=3)
    with session_factory(pg_engine)() as s:
        for row in offline.iter_rows(named=True):
            got = history_primitives(s, row["dest_id"], row["orig_id"], row["step"], (24, 168))
            for col in primitive_columns((24, 168)):
                want = row[col]
                if want is None or (isinstance(want, float) and math.isnan(want)):
                    assert got[col] is None
                else:
                    assert got[col] == pytest.approx(want, rel=1e-9, abs=1e-6)


def test_concurrent_duplicate_scores_store_one_prediction(pg_engine, artifacts):
    """Real race on PostgreSQL: 8 threads submit the same transaction at once."""
    from sqlalchemy import func, select

    from riskstream.api.schemas.scoring import ScoreRequest
    from riskstream.config import AppConfig
    from riskstream.features.registry import get_registry
    from riskstream.inference.model_loader import load_serving_model
    from riskstream.inference.predictor import ScoringService
    from riskstream.storage.database import create_db_engine, session_factory
    from riskstream.storage.models import PredictionRow
    from tests.conftest import txn

    cfg = AppConfig.load()
    engine = create_db_engine(URL, cfg)
    model = load_serving_model(artifacts["bundle"], artifacts["policy"], get_registry(), False)
    service = ScoringService(session_factory(engine), model, get_registry(), cfg, "it")
    barrier = threading.Barrier(8)

    def go(_):
        barrier.wait()
        return service.score(ScoreRequest(**txn("race-1")), "it")

    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        results = list(pool.map(go, range(8)))
    assert sum(not r.idempotent_replay for r in results) == 1
    assert len({r.fraud_probability for r in results}) == 1
    with session_factory(engine)() as s:
        assert s.execute(select(func.count()).select_from(PredictionRow)).scalar_one() == 1
    engine.dispose()


def test_statement_timeout_is_applied(pg_engine):
    from sqlalchemy import text
    from sqlalchemy.exc import OperationalError

    from riskstream.config import AppConfig
    from riskstream.storage.database import create_db_engine

    engine = create_db_engine(URL, AppConfig.load(db_statement_timeout_ms=200))
    with pytest.raises(OperationalError), engine.connect() as conn:
        conn.execute(text("SELECT pg_sleep(2)"))
    engine.dispose()
