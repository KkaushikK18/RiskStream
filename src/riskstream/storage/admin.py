"""Database administration: migrations and historical backfill.

Backfill loads canonical transactions (no predictions) so behavioral features
have history to read. Without it, every entity looks new to the service,
which is a real distribution shift from training (models saw a month of
history). The backfill is re-runnable: existing transaction ids are skipped.

PostgreSQL path: COPY into a temporary table, then INSERT ... ON CONFLICT DO
NOTHING. This moves about a million rows per few seconds instead of hours of
row-by-row inserts.
"""

from __future__ import annotations

import io
import math
import time
from datetime import timedelta

import polars as pl
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text

from riskstream.config import PROJECT_ROOT, AppConfig
from riskstream.data.ingestion import load_interim
from riskstream.logging_config import get_logger

logger = get_logger(__name__)

BACKFILL_COLUMNS = ["transaction_id", "event_time", "step", "type", "amount", "log_amount", "orig_id",
                    "orig_balance_before", "dest_id", "dest_type", "source"]


def alembic_config(url: str) -> Config:
    cfg = Config(str(PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(PROJECT_ROOT / "src" / "riskstream" / "storage" / "migrations"))
    cfg.attributes["database_url"] = url
    return cfg


def upgrade(url: str, revision: str = "head") -> None:
    command.upgrade(alembic_config(url), revision)


def backfill_frame(lf: pl.LazyFrame, from_step: int | None, until_step: int | None,
                   epoch_cfg: AppConfig) -> pl.LazyFrame:
    """Canonical transactions -> ``transactions`` table rows."""
    if from_step is not None:
        lf = lf.filter(pl.col("step") >= from_step)
    if until_step is not None:
        lf = lf.filter(pl.col("step") <= until_step)
    epoch = epoch_cfg.simulation_epoch
    return lf.sort("step", "event_seq").select(
        "transaction_id",
        (pl.lit(epoch) + pl.duration(hours=pl.col("step") - 1)).dt.convert_time_zone("UTC").alias("event_time"),
        "step", "type", pl.col("amount").round(2), pl.col("amount").log1p().alias("log_amount"),
        "orig_id", pl.col("orig_balance_before").round(2), "dest_id", "dest_type",
        pl.lit("historical_backfill").alias("source"),
    )


def seed_history(engine: Engine, dataset: str, *, from_step: int | None = None, until_step: int | None = None,
                 chunk_rows: int = 250_000, canonical: pl.LazyFrame | None = None) -> dict:
    cfg = AppConfig.load()
    lf = canonical if canonical is not None else load_interim(dataset)
    frame = backfill_frame(lf, from_step, until_step, cfg).collect()
    total = len(frame)
    logger.info("Backfilling %s rows of %s (steps %s..%s)", f"{total:,}", dataset, from_step or "start",
                until_step or "end")
    start = time.perf_counter()
    inserted = 0
    if engine.dialect.name == "postgresql":
        inserted = _copy_postgres(engine, frame, chunk_rows)
    else:
        inserted = _insert_generic(engine, frame, chunk_rows)
    if engine.dialect.name == "postgresql":
        with engine.begin() as conn:
            conn.execute(text("ANALYZE transactions"))
    elapsed = time.perf_counter() - start
    logger.info("Backfill done: %s new rows, %s already present, %.1fs", f"{inserted:,}",
                f"{total - inserted:,}", elapsed)
    return {"rows_considered": total, "rows_inserted": inserted, "seconds": round(elapsed, 1)}


def _copy_postgres(engine: Engine, frame: pl.DataFrame, chunk_rows: int) -> int:
    inserted = 0
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute("CREATE TEMP TABLE backfill_stage (LIKE transactions INCLUDING DEFAULTS) ON COMMIT DROP")
        cols = ", ".join(BACKFILL_COLUMNS)
        for i in range(math.ceil(len(frame) / chunk_rows)):
            chunk = frame.slice(i * chunk_rows, chunk_rows)
            buf = io.BytesIO()
            chunk.write_csv(buf, include_header=False, datetime_format="%Y-%m-%dT%H:%M:%S%z")
            with cur.copy(f"COPY backfill_stage ({cols}) FROM STDIN WITH (FORMAT csv)") as copy:
                copy.write(buf.getvalue())
        cur.execute(f"INSERT INTO transactions ({cols}) SELECT {cols} FROM backfill_stage "
                    "ON CONFLICT (transaction_id) DO NOTHING")
        inserted = cur.rowcount
        raw.commit()
    finally:
        raw.close()
    return inserted


def _insert_generic(engine: Engine, frame: pl.DataFrame, chunk_rows: int) -> int:
    cols = ", ".join(BACKFILL_COLUMNS)
    params = ", ".join(f":{c}" for c in BACKFILL_COLUMNS)
    stmt = text(f"INSERT OR IGNORE INTO transactions ({cols}) VALUES ({params})")
    inserted = 0
    with engine.begin() as conn:
        before = conn.execute(text("SELECT COUNT(*) FROM transactions")).scalar_one()
        for i in range(math.ceil(len(frame) / chunk_rows)):
            conn.execute(stmt, frame.slice(i * chunk_rows, chunk_rows).to_dicts())
        inserted = conn.execute(text("SELECT COUNT(*) FROM transactions")).scalar_one() - before
    return int(inserted)


def latest_stored_step(engine_or_conn) -> int | None:
    """Highest step in ``transactions`` (uses ix_transactions_step)."""
    with _connect(engine_or_conn) as conn:
        return conn.execute(text("SELECT max(step) FROM transactions")).scalar_one()


def history_gaps(engine_or_conn, canonical: pl.LazyFrame, until_step: int) -> list[int]:
    """Steps <= until_step whose stored row count differs from the dataset's.

    An empty list means the database holds exactly the dataset's history up to
    ``until_step``, which is what the training features assumed.
    """
    expected = dict(canonical.filter(pl.col("step") <= until_step).group_by("step").len()
                    .collect().iter_rows())
    with _connect(engine_or_conn) as conn:
        stored = dict(conn.execute(text(
            "SELECT step, count(*) FROM transactions WHERE step <= :s GROUP BY step"), {"s": until_step}).all())
    return sorted(s for s in set(expected) | set(stored) if expected.get(s, 0) != stored.get(s, 0))


class _connect:
    """Accept an Engine (open and close a connection) or an existing Connection."""

    def __init__(self, target) -> None:
        self.target, self.conn = target, None

    def __enter__(self):
        if isinstance(self.target, Engine):
            self.conn = self.target.connect()
            return self.conn
        return self.target

    def __exit__(self, *exc) -> None:
        if self.conn is not None:
            self.conn.close()


def delete_scored(engine: Engine, *, from_step: int, actor: str) -> dict:
    """Delete API-scored transactions at step >= from_step, with their predictions,
    labels and outbox events. Backfilled history is never touched.

    Needs the owner role: the application role has no DELETE privilege by design.
    Writes an ``admin_delete_scored`` audit event in the same transaction.
    """
    from sqlalchemy.orm import Session

    from riskstream.storage.repositories.events import add_audit_event

    ids_sql = "SELECT transaction_id FROM transactions WHERE source = 'api' AND step >= :s"
    with engine.begin() as conn:
        ids = [r[0] for r in conn.execute(text(ids_sql), {"s": from_step})]
        counts = {"transactions": len(ids)}
        if ids:
            for table in ("predictions", "labels"):
                counts[table] = conn.execute(text(
                    f"DELETE FROM {table} WHERE transaction_id IN ({ids_sql})"), {"s": from_step}).rowcount
            outbox = 0
            for i in range(0, len(ids), 1000):
                chunk = ids[i:i + 1000]
                params = {f"i{n}": v for n, v in enumerate(chunk)}
                placeholders = ", ".join(f":i{n}" for n in range(len(chunk)))
                # payload->>'transaction_id' on PostgreSQL; json_extract on SQLite
                key = ("payload->>'transaction_id'" if engine.dialect.name == "postgresql"
                       else "json_extract(payload, '$.transaction_id')")
                outbox += conn.execute(text(
                    f"DELETE FROM outbox_events WHERE {key} IN ({placeholders})"), params).rowcount
            counts["outbox_events"] = outbox
            conn.execute(text("DELETE FROM transactions WHERE source = 'api' AND step >= :s"),
                         {"s": from_step})
        with Session(bind=conn) as s:
            add_audit_event(s, "admin_delete_scored", actor, f"step>={from_step}", counts)
            s.flush()
    logger.info("Deleted scored rows from step %d: %s", from_step, counts)
    return counts


def admin_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True)


def step_to_timestamp(step: int, cfg: AppConfig) -> str:
    return (cfg.simulation_epoch + timedelta(hours=step - 1)).isoformat().replace("+00:00", "Z")
