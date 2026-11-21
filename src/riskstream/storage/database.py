"""Engine and session factory with pooling and timeouts.

PostgreSQL connections get server-side ``statement_timeout`` and
``lock_timeout`` so a slow query fails fast instead of holding a worker thread.
``pool_timeout`` bounds how long a request waits for a free connection; when
it expires the request fails with 503 (see ``api.errors``) instead of queueing.

SQLite is supported for unit tests only.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

from sqlalchemy import Engine, create_engine, event, text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from riskstream.config import AppConfig

DATABASE_ERRORS: tuple[type[Exception], ...] = (OperationalError, PoolTimeoutError, DBAPIError)


def create_db_engine(url: str, cfg: AppConfig | None = None) -> Engine:
    cfg = cfg or AppConfig.load()
    if url.startswith("sqlite"):
        engine = create_engine(
            url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool if ":memory:" in url or url.endswith("sqlite://") else None,
        )

        @event.listens_for(engine, "connect")
        def _fk_on(dbapi_conn, _):  # pragma: no cover - trivial
            dbapi_conn.execute("PRAGMA foreign_keys=ON")

        return engine

    options = f"-c statement_timeout={cfg.db_statement_timeout_ms} -c lock_timeout={cfg.db_lock_timeout_ms}"
    return create_engine(
        url,
        pool_size=cfg.db_pool_size,
        max_overflow=cfg.db_max_overflow,
        pool_timeout=cfg.db_pool_timeout_s,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={"connect_timeout": cfg.db_connect_timeout_s, "options": options,
                      "application_name": "riskstream-api"},
    )


def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


@contextmanager
def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    session = factory()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def ping(engine: Engine) -> float:
    """Round-trip ``SELECT 1``; returns latency in milliseconds."""
    import time

    t = time.perf_counter()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return (time.perf_counter() - t) * 1000


def utc(dt: datetime | None) -> datetime | None:
    """SQLite drops tzinfo; every datetime leaving storage is UTC-aware."""
    if dt is None:
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def utcnow() -> datetime:
    return datetime.now(UTC)
