"""Phase 4 CLI commands (registered in riskstream.cli)."""

from __future__ import annotations

import argparse
import json
import math
import time
from typing import Any

import polars as pl

from riskstream.config import AppConfig, Settings, StreamingConfig, get_settings
from riskstream.logging_config import get_logger

logger = get_logger("riskstream.streaming.cli")

FEATURE_GROUP = "riskstream-feature-updater"
SCORING_GROUP = "riskstream-scoring-worker"


def _need(value: str | None, name: str) -> str:
    if not value:
        raise SystemExit(f"{name} is not set (see .env.example)")
    return value


def _store(settings: Settings, cfg: StreamingConfig):
    from riskstream.features.online_store import RedisFeatureStore
    from riskstream.features.registry import get_registry

    return RedisFeatureStore.from_url(_need(settings.redis_url, "REDIS_URL"), cfg, get_registry().config.windows)


def _metrics(port: int | None) -> None:
    if port:
        from prometheus_client import start_http_server

        start_http_server(port, addr="127.0.0.1")
        logger.info("metrics on http://127.0.0.1:%d/metrics", port)


def cmd_topics_create(args: argparse.Namespace) -> int:
    from riskstream.streaming.kafka import ensure_topics

    print(json.dumps(ensure_topics(_need(get_settings().kafka_bootstrap_servers, "KAFKA_BOOTSTRAP_SERVERS"),
                                   StreamingConfig.load()), indent=2))
    return 0


def cmd_online_bootstrap(args: argparse.Namespace) -> int:
    from riskstream.features.online_bootstrap import bootstrap
    from riskstream.storage.admin import admin_engine

    settings, cfg = get_settings(), StreamingConfig.load()
    store = _store(settings, cfg)
    engine = admin_engine(_need(settings.admin_database_url, "MIGRATION_DATABASE_URL"))
    try:
        print(json.dumps(bootstrap(store, engine), indent=2))
    finally:
        engine.dispose()
    return 0


def cmd_run_relay(args: argparse.Namespace) -> int:
    from riskstream.storage.database import create_db_engine
    from riskstream.streaming.consumers import StopFlag
    from riskstream.streaming.kafka import producer
    from riskstream.streaming.outbox_relay import OutboxRelay

    settings, cfg = get_settings(), StreamingConfig.load()
    _metrics(args.metrics_port)
    engine = create_db_engine(_need(settings.database_url, "DATABASE_URL"))
    prod = producer(_need(settings.kafka_bootstrap_servers, "KAFKA_BOOTSTRAP_SERVERS"), cfg,
                    f"outbox-relay-{settings.instance_id}")
    from riskstream.streaming.kafka import GroupLagProbe

    store = _store(settings, cfg) if settings.redis_url else None
    probe = GroupLagProbe(settings.kafka_bootstrap_servers, FEATURE_GROUP, cfg.topic_scored)
    stop = StopFlag()
    stop.install()
    try:
        totals = OutboxRelay(engine, prod, cfg, store=store, lag_probe=probe).run(stop, until_empty=args.until_empty)
    finally:
        prod.flush(10)
        probe.close()
        engine.dispose()
    print(json.dumps(totals))
    return 0


def _runner(processor: Any, group: str, topic: str, settings: Settings, cfg: StreamingConfig, client_id: str):
    from riskstream.streaming.consumers import ConsumerRunner, StopFlag
    from riskstream.streaming.dead_letter import DeadLetterPublisher
    from riskstream.streaming.kafka import consumer, consumer_lag, producer

    bootstrap = _need(settings.kafka_bootstrap_servers, "KAFKA_BOOTSTRAP_SERVERS")
    c = consumer(bootstrap, cfg, group, client_id)
    c.subscribe([topic])
    dlq = DeadLetterPublisher(producer(bootstrap, cfg, f"{client_id}-dlq"), cfg.topic_dlq, processor.name, group)
    stop = StopFlag()
    stop.install()
    return ConsumerRunner(c, processor, dlq, cfg, stop=stop, lag_fn=lambda: consumer_lag(c))


def cmd_run_feature_consumer(args: argparse.Namespace) -> int:
    from riskstream.streaming.consumers import FeatureUpdateProcessor

    settings, cfg = get_settings(), StreamingConfig.load()
    _metrics(args.metrics_port)
    runner = _runner(FeatureUpdateProcessor(_store(settings, cfg)), args.group, cfg.topic_scored, settings, cfg,
                     f"feature-updater-{settings.instance_id}")
    print(json.dumps(runner.run(max_messages=args.max_messages, idle_exit_s=args.idle_exit)))
    return 0


def cmd_run_scoring_worker(args: argparse.Namespace) -> int:
    import httpx

    from riskstream.streaming.consumers import ScoringProcessor

    settings, cfg = get_settings(), StreamingConfig.load()
    _metrics(args.metrics_port)
    client = httpx.Client(base_url=settings.api_base_url, timeout=httpx.Timeout(10.0, connect=2.0))
    runner = _runner(ScoringProcessor(client, _need(settings.worker_api_key, "WORKER_API_KEY")), args.group,
                     cfg.topic_ingested, settings, cfg, f"scoring-worker-{settings.instance_id}")
    try:
        print(json.dumps(runner.run(max_messages=args.max_messages, idle_exit_s=args.idle_exit)))
    finally:
        client.close()
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    from riskstream.data.ingestion import load_interim
    from riskstream.streaming.kafka import producer
    from riskstream.streaming.replay import replay

    settings, cfg, app_cfg = get_settings(), StreamingConfig.load(), AppConfig.load()
    last = args.to_step if args.to_step is not None else args.from_step + args.steps - 1
    rows = (load_interim(args.dataset).filter(pl.col("step").is_between(args.from_step, last))
            .sort("step", "event_seq").collect())
    if args.limit:
        rows = rows.head(args.limit)
    if len(rows) == 0:
        raise SystemExit(f"no rows in steps {args.from_step}-{last}")
    store = _store(settings, cfg) if args.barrier else None
    if args.barrier:
        from riskstream.storage.admin import admin_engine, latest_stored_step

        engine = admin_engine(_need(settings.admin_database_url, "MIGRATION_DATABASE_URL"))
        latest = latest_stored_step(engine)
        engine.dispose()
        if latest is not None and args.from_step <= latest:
            raise SystemExit(f"--barrier needs steps after the stored history (latest stored step {latest}); "
                             f"start at {latest + 1}")
    prod = producer(_need(settings.kafka_bootstrap_servers, "KAFKA_BOOTSTRAP_SERVERS"), cfg, "replay")
    rate = args.rate if (args.rate or args.step_seconds) else None
    if rate is None and not args.step_seconds and not args.max_speed:
        rate = cfg.replay_rate_per_second
    print(f"replaying {len(rows):,} transactions, steps {args.from_step}-{last}, "
          f"{'barrier' if args.barrier else 'no barrier'}, "
          f"{'max speed' if args.max_speed else (f'{rate}/s' if rate else f'{args.step_seconds}s per step')}")
    print(json.dumps(replay(rows, prod, cfg, app_cfg, rate=rate, step_seconds=args.step_seconds,
                            barrier_store=store, barrier_timeout_s=args.barrier_timeout), indent=2))
    return 0


def cmd_stream_status(args: argparse.Namespace) -> int:
    from sqlalchemy import text

    from riskstream.storage.admin import admin_engine
    from riskstream.streaming.kafka import group_lag

    settings, cfg = get_settings(), StreamingConfig.load()
    out: dict[str, Any] = {}
    engine = admin_engine(_need(settings.admin_database_url, "MIGRATION_DATABASE_URL"))
    with engine.connect() as conn:
        n, oldest = conn.execute(text(
            "SELECT count(*), min(created_at) FROM outbox_events WHERE published_at IS NULL")).one()
        out["outbox"] = {"unpublished": n, "oldest_unpublished": str(oldest) if oldest else None}
    engine.dispose()
    if settings.kafka_bootstrap_servers:
        bs = settings.kafka_bootstrap_servers
        out["lag"] = {k: group_lag(bs, g, t)["total_lag"] for k, g, t in (
            ("feature_updater", FEATURE_GROUP, cfg.topic_scored), ("scoring_worker", SCORING_GROUP, cfg.topic_ingested))}
        from confluent_kafka import Consumer, TopicPartition

        c = Consumer({"bootstrap.servers": bs, "group.id": "riskstream-status"})
        md = c.list_topics(timeout=10).topics
        out["topics"] = {}
        for t in (cfg.topic_ingested, cfg.topic_scored, cfg.topic_labeled, cfg.topic_dlq):
            if t in md:
                out["topics"][t] = sum(c.get_watermark_offsets(TopicPartition(t, p), timeout=10)[1]
                                       for p in md[t].partitions)
        c.close()
    if settings.redis_url:
        store = _store(settings, cfg)
        out["online_store"] = store.status()
    print(json.dumps(out, indent=2, default=str))
    return 0


def _dlq_records(bootstrap: str, cfg: StreamingConfig, limit: int, timeout_s: float = 5.0) -> list[tuple]:
    import uuid

    from confluent_kafka import Consumer

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"dlq-inspect-{uuid.uuid4().hex[:6]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([cfg.topic_dlq])
    out, idle = [], time.monotonic()
    try:
        while len(out) < limit and time.monotonic() - idle < timeout_s:
            msg = c.poll(0.5)
            if msg is None or msg.error():
                continue
            idle = time.monotonic()
            out.append((msg, json.loads(msg.value())))
    finally:
        c.close()
    return out


def cmd_dlq_inspect(args: argparse.Namespace) -> int:
    settings, cfg = get_settings(), StreamingConfig.load()
    recs = _dlq_records(_need(settings.kafka_bootstrap_servers, "KAFKA_BOOTSTRAP_SERVERS"), cfg, args.limit)
    print(f"{len(recs)} dead-letter record(s)")
    for _, r in recs:
        print(json.dumps({k: r[k] for k in ("dlq_id", "failed_at", "consumer", "original_topic",
                                            "original_partition", "original_offset", "error_class", "error")}))
    return 0


def cmd_dlq_replay(args: argparse.Namespace) -> int:
    from riskstream.streaming.dead_letter import original_value
    from riskstream.streaming.kafka import producer

    if not args.yes:
        raise SystemExit("Refusing to re-publish without --yes")
    settings, cfg = get_settings(), StreamingConfig.load()
    bs = _need(settings.kafka_bootstrap_servers, "KAFKA_BOOTSTRAP_SERVERS")
    prod = producer(bs, cfg, "dlq-replay")
    n = 0
    for _, r in _dlq_records(bs, cfg, args.limit):
        key = r["original_key"].encode() if r.get("original_key") else None
        prod.produce(r["original_topic"], key=key, value=original_value(r))
        n += 1
    prod.flush(30)
    print(f"re-published {n} record(s) to their original topics (the DLQ itself is append-only)")
    return 0


def cmd_check_online_store(args: argparse.Namespace) -> int:
    """Read-only: Redis primitives vs PostgreSQL history for the next step's transactions."""
    from riskstream.data.ingestion import load_interim
    from riskstream.features.derive import primitive_columns
    from riskstream.features.online_store import FeatureStoreNotUsable
    from riskstream.storage.admin import admin_engine, latest_stored_step
    from riskstream.storage.database import session_factory
    from riskstream.storage.repositories.feature_history import history_primitives

    settings, cfg = get_settings(), StreamingConfig.load()
    store = _store(settings, cfg)
    engine = admin_engine(_need(settings.admin_database_url, "MIGRATION_DATABASE_URL"))
    step = args.step or (latest_stored_step(engine) or 0) + 1
    rows = load_interim(args.dataset).filter(pl.col("step") == step).collect()
    if args.limit:
        rows = rows.head(args.limit)
    cols = primitive_columns(store.windows)
    mismatches, refused, t_redis, t_pg = [], {}, [], []
    with session_factory(engine)() as s:
        for r in rows.iter_rows(named=True):
            t0 = time.perf_counter()
            try:
                a = store.primitives(r["dest_id"], r["orig_id"], step, check_freshness=not args.ignore_freshness)
            except FeatureStoreNotUsable as exc:
                refused[exc.reason] = refused.get(exc.reason, 0) + 1
                continue
            t_redis.append((time.perf_counter() - t0) * 1000)
            t0 = time.perf_counter()
            b = history_primitives(s, r["dest_id"], r["orig_id"], step, store.windows)
            t_pg.append((time.perf_counter() - t0) * 1000)
            for col in cols:
                x, y = a[col], b[col]
                same = (x is None and y is None) or (x is not None and y is not None and math.isclose(
                    float(x), float(y), rel_tol=1e-9, abs_tol=1e-6))
                if not same:
                    mismatches.append({"transaction_id": r["transaction_id"], "primitive": col, "redis": x,
                                       "postgres": y})
    engine.dispose()

    def pct(v: list[float], q: float) -> float | None:
        return round(sorted(v)[int(q * (len(v) - 1))], 3) if v else None

    print(json.dumps({"step": step, "transactions": len(rows), "compared": len(t_redis),
                      "primitives_compared": len(t_redis) * len(cols), "mismatches": len(mismatches),
                      "mismatch_examples": mismatches[:10], "refused": refused,
                      "redis_ms": {"p50": pct(t_redis, .5), "p95": pct(t_redis, .95), "p99": pct(t_redis, .99)},
                      "postgres_ms": {"p50": pct(t_pg, .5), "p95": pct(t_pg, .95), "p99": pct(t_pg, .99)}},
                     indent=2))
    return 0 if not mismatches else 2


def cmd_verify_predictions(args: argparse.Namespace) -> int:
    """Stored prediction features for a step range vs the offline feature tables."""
    from sqlalchemy import select

    from riskstream.data.splitting import SPLITS
    from riskstream.features.offline import load_features
    from riskstream.storage.admin import admin_engine
    from riskstream.storage.database import session_factory
    from riskstream.storage.models import PredictionRow, TransactionRow

    settings = get_settings()
    engine = admin_engine(_need(settings.admin_database_url, "MIGRATION_DATABASE_URL"))
    with session_factory(engine)() as s:
        rows = s.execute(select(PredictionRow.transaction_id, PredictionRow.features, PredictionRow.feature_source,
                                PredictionRow.feature_fallback_reason, PredictionRow.feature_latency_ms)
                         .join(TransactionRow, TransactionRow.transaction_id == PredictionRow.transaction_id)
                         .where(TransactionRow.step.between(args.from_step, args.to_step))).all()
    engine.dispose()
    ids = [r[0] for r in rows]
    offline = pl.concat([load_features(args.dataset, sp).filter(pl.col("transaction_id").is_in(ids))
                         for sp in SPLITS])
    off = {r["transaction_id"]: r for r in offline.iter_rows(named=True)}
    names = [c for c in offline.columns if c not in ("transaction_id", "step", "event_seq", "type", "is_fraud")]
    sources: dict[str, int] = {}
    reasons: dict[str, int] = {}
    lat: dict[str, list[float]] = {}
    mismatches = []
    for tid, feats, source, reason, ms in rows:
        sources[source] = sources.get(source, 0) + 1
        lat.setdefault(source, []).append(ms)
        if reason:
            reasons[reason] = reasons.get(reason, 0) + 1
        ref = off.get(tid)
        if ref is None:
            mismatches.append({"transaction_id": tid, "feature": "*", "detail": "not in offline tables"})
            continue
        for n in names:
            a, b = feats.get(n), ref[n]
            a = float("nan") if a is None else a
            if math.isnan(a) and math.isnan(b):
                continue
            if math.isnan(a) != math.isnan(b) or abs(a - b) > 1e-4 * max(1.0, abs(b)):
                mismatches.append({"transaction_id": tid, "feature": n, "served": a, "offline": b})
    print(json.dumps({"steps": [args.from_step, args.to_step], "predictions": len(rows),
                      "features_compared": len(rows) * len(names), "mismatches": len(mismatches),
                      "mismatch_examples": mismatches[:10], "feature_source": sources, "fallback_reasons": reasons,
                      "feature_ms_p50_by_source": {k: round(sorted(v)[len(v) // 2], 2) for k, v in lat.items()}},
                     indent=2))
    return 0 if not mismatches else 2


def register(sub: Any) -> None:
    p = sub.add_parser("topics-create", help="Create Kafka/Redpanda topics (idempotent)")
    p.set_defaults(func=cmd_topics_create)

    p = sub.add_parser("online-bootstrap", help="Rebuild Redis online features from a PostgreSQL snapshot")
    p.set_defaults(func=cmd_online_bootstrap)

    p = sub.add_parser("run-relay", help="Publish outbox events to Kafka")
    p.add_argument("--metrics-port", type=int)
    p.add_argument("--until-empty", action="store_true", help="Exit when the backlog is empty")
    p.set_defaults(func=cmd_run_relay)

    for name, func, group, help_ in (
        ("run-feature-consumer", cmd_run_feature_consumer, FEATURE_GROUP, "transactions.scored -> Redis"),
        ("run-scoring-worker", cmd_run_scoring_worker, SCORING_GROUP, "transactions.ingested -> POST /v1/score"),
    ):
        p = sub.add_parser(name, help=help_)
        p.add_argument("--metrics-port", type=int)
        p.add_argument("--group", default=group)
        p.add_argument("--max-messages", type=int)
        p.add_argument("--idle-exit", type=float, help="Exit after this many idle seconds")
        p.set_defaults(func=func)

    p = sub.add_parser("replay", help="Publish dataset transactions to transactions.ingested")
    p.add_argument("--dataset", default="paysim", choices=["paysim", "synthetic"])
    p.add_argument("--from-step", type=int, required=True)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--steps", type=int, default=1)
    g.add_argument("--to-step", type=int)
    pace = p.add_mutually_exclusive_group()
    pace.add_argument("--rate", type=float, help="Events per second")
    pace.add_argument("--step-seconds", type=float, help="Wall-clock seconds per simulated hour")
    pace.add_argument("--max-speed", action="store_true")
    p.add_argument("--barrier", action="store_true",
                   help="Wait until each hour is applied to Redis before publishing the next")
    p.add_argument("--barrier-timeout", type=float)
    p.add_argument("--limit", type=int)
    p.set_defaults(func=cmd_replay)

    p = sub.add_parser("stream-status", help="Outbox backlog, consumer lag, topic sizes, online store state")
    p.set_defaults(func=cmd_stream_status)

    p = sub.add_parser("dlq-inspect", help="Show dead-letter records")
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(func=cmd_dlq_inspect)

    p = sub.add_parser("dlq-replay", help="Re-publish dead-letter payloads to their original topics")
    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--yes", action="store_true")
    p.set_defaults(func=cmd_dlq_replay)

    p = sub.add_parser("check-online-store", help="Read-only: Redis vs PostgreSQL primitives for one step")
    p.add_argument("--dataset", default="paysim", choices=["paysim", "synthetic"])
    p.add_argument("--step", type=int, help="Default: the step after the stored history")
    p.add_argument("--limit", type=int)
    p.add_argument("--ignore-freshness", action="store_true",
                   help="Compare even if relay/consumer heartbeats are stale (data check only)")
    p.set_defaults(func=cmd_check_online_store)

    p = sub.add_parser("verify-predictions", help="Stored prediction features vs offline features")
    p.add_argument("--dataset", default="paysim", choices=["paysim", "synthetic"])
    p.add_argument("--from-step", type=int, required=True)
    p.add_argument("--to-step", type=int, required=True)
    p.set_defaults(func=cmd_verify_predictions)
