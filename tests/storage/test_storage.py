"""Migrations, schema drift and SQL feature-history parity (SQLite; PostgreSQL variant in tests/integration)."""

import math

import polars as pl
import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, inspect, text

from riskstream.features.derive import primitive_columns
from riskstream.features.offline import compute_primitives
from riskstream.storage.admin import seed_history, upgrade
from riskstream.storage.database import session_factory
from riskstream.storage.models import Base
from riskstream.storage.repositories.feature_history import history_primitives

WINDOWS = (24, 168)


@pytest.fixture
def migrated_url(tmp_path):
    url = f"sqlite:///{(tmp_path / 'rs.db').as_posix()}"
    upgrade(url)
    return url


def test_migration_creates_all_tables(migrated_url):
    names = set(inspect(create_engine(migrated_url)).get_table_names())
    assert {"transactions", "predictions", "labels", "model_deployments", "model_evaluations",
            "outbox_events", "audit_events", "alembic_version"} <= names


def test_migration_matches_orm_models(migrated_url):
    engine = create_engine(migrated_url)
    with engine.connect() as conn:
        diff = compare_metadata(MigrationContext.configure(conn), Base.metadata)
    assert diff == [], diff


def test_downgrade_removes_tables(migrated_url):
    from alembic import command

    from riskstream.storage.admin import alembic_config

    command.downgrade(alembic_config(migrated_url), "base")
    assert "transactions" not in inspect(create_engine(migrated_url)).get_table_names()


def test_backfill_is_idempotent(migrated_url, small_canonical):
    engine = create_engine(migrated_url)
    first = seed_history(engine, "synthetic", canonical=small_canonical, until_step=30)
    second = seed_history(engine, "synthetic", canonical=small_canonical, until_step=30)
    expected = small_canonical.filter(pl.col("step") <= 30).select(pl.len()).collect().item()
    assert first["rows_inserted"] == expected
    assert second["rows_inserted"] == 0
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM transactions")).scalar_one() == expected


def test_sql_history_matches_offline_primitives(migrated_url, small_canonical):
    """The serving path's SQL primitives equal the offline as-of-join primitives."""
    engine = create_engine(migrated_url)
    seed_history(engine, "synthetic", canonical=small_canonical)
    offline = compute_primitives(small_canonical, WINDOWS).collect()
    busy = offline.group_by("dest_id").len().sort("len", descending=True).head(25)["dest_id"]
    sample = pl.concat([offline.filter(pl.col("dest_id").is_in(busy.implode())).head(300),
                        offline.sample(200, seed=1)])
    sessions = session_factory(engine)
    checked = 0
    with sessions() as s:
        for row in sample.iter_rows(named=True):
            got = history_primitives(s, row["dest_id"], row["orig_id"], row["step"], WINDOWS)
            for col in primitive_columns(WINDOWS):
                a, b = got[col], row[col]
                if b is None or (isinstance(b, float) and math.isnan(b)):
                    assert a is None, (col, a, b)
                else:
                    assert a == pytest.approx(b, rel=1e-9, abs=1e-6), (row["transaction_id"], col, a, b)
                checked += 1
    assert checked == len(sample) * len(primitive_columns(WINDOWS))


def test_same_step_rows_excluded_from_history(migrated_url, tmp_path):
    from tests.conftest import raw_rows, to_canonical

    lf = to_canonical(raw_rows([{"step": 5, "nameDest": "CD", "amount": 1.0, "nameOrig": f"C{i}"} for i in range(3)]
                               + [{"step": 4, "nameDest": "CD", "amount": 7.0}]), tmp_path)
    engine = create_engine(migrated_url)
    seed_history(engine, "synthetic", canonical=lf)
    with session_factory(engine)() as s:
        p = history_primitives(s, "CD", "C0", 5, WINDOWS)
    assert p["dest_p_cnt_24"] == 1 and p["dest_p_sum_24"] == pytest.approx(7.0)
    assert p["dest_p_last_step"] == 4
