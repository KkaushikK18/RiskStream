"""Command-line entry point: ``python -m riskstream.cli <command>``."""

from __future__ import annotations

import argparse
import json
import sys
import time

from riskstream.config import dataset_config, ensure_directories, load_config, resolve_path
from riskstream.logging_config import get_logger, setup_logging

logger = get_logger("riskstream.cli")


def cmd_generate_synthetic(args: argparse.Namespace) -> int:
    from riskstream.data import synthetic_generator as sg

    cfg = sg.SyntheticConfig.from_yaml(
        seed=args.seed, num_transactions=args.num_transactions, num_steps=args.num_steps
    )
    df = sg.generate(cfg)
    path = sg.write(df, resolve_path(dataset_config("synthetic")["raw_path"]))
    logger.info(
        "Wrote %s synthetic rows (%s fraud) to %s [seed=%d]",
        f"{len(df):,}", int(df["isFraud"].sum()), path, cfg.seed,
    )
    return 0


def cmd_prepare_data(args: argparse.Namespace) -> int:
    from riskstream.data.eda import run_eda
    from riskstream.data.ingestion import ingest, load_interim
    from riskstream.data.splitting import run_split

    t0 = time.perf_counter()
    result = ingest(args.dataset)
    lf = load_interim(args.dataset)
    if not args.skip_eda:
        eda_cfg = load_config("data")["eda"]
        run_eda(
            lf,
            resolve_path(dataset_config(args.dataset)["eda_dir"]),
            args.dataset,
            result.manifest["is_synthetic_data"],
            eda_cfg["correlation_sample_rows"],
            eda_cfg["sample_seed"],
        )
    run_split(args.dataset, lf)
    logger.info("prepare-data finished in %.1fs", time.perf_counter() - t0)
    return 0


def cmd_build_features(args: argparse.Namespace) -> int:
    from riskstream.features.offline import run_build_features

    run_build_features(args.dataset)
    return 0


def cmd_train(args: argparse.Namespace) -> int:
    from riskstream.training.pipeline import run_training

    result = run_training(
        args.dataset,
        models=args.models,
        n_trials=args.n_trials,
        run_name=args.run_name,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_tune_policy(args: argparse.Namespace) -> int:
    from pathlib import Path

    from riskstream.config import PROJECT_ROOT
    from riskstream.training.policy import run_policy_tuning

    if args.bundle:
        bundle = Path(args.bundle)
    else:
        candidates = sorted((PROJECT_ROOT / "models" / "candidates").glob(f"*-{args.dataset}-*"))
        if not candidates:
            raise FileNotFoundError(f"No candidate bundle for {args.dataset}; run `train` first")
        bundle = candidates[-1]
    print(json.dumps(run_policy_tuning(args.dataset, bundle), indent=2))
    return 0


def _require_url(url: str | None, what: str) -> str:
    if not url:
        raise SystemExit(f"{what} is not set (see .env.example)")
    return url


def cmd_create_api_key(args: argparse.Namespace) -> int:
    from riskstream.api.security import ROLES, generate_key, hash_key

    roles = [r.strip() for r in args.roles.split(",") if r.strip()]
    bad = [r for r in roles if r not in ROLES]
    if bad:
        raise SystemExit(f"Unknown roles {bad}; choose from {list(ROLES)}")
    key = generate_key()
    entry = {"name": args.name, "roles": roles, "sha256": hash_key(key)}
    print("API key (shown once; give it to the client, do not commit it):")
    print(f"  {key}")
    print("Add this entry to the API_KEYS JSON list in .env:")
    print(f"  {json.dumps(entry)}")
    return 0


def cmd_db_upgrade(args: argparse.Namespace) -> int:
    from riskstream.config import get_settings
    from riskstream.storage.admin import upgrade

    upgrade(_require_url(get_settings().admin_database_url, "MIGRATION_DATABASE_URL / DATABASE_URL"), args.revision)
    logger.info("Database migrated to %s", args.revision)
    return 0


def cmd_seed_history(args: argparse.Namespace) -> int:
    from riskstream.config import get_settings
    from riskstream.storage.admin import admin_engine, seed_history

    url = _require_url(get_settings().admin_database_url, "MIGRATION_DATABASE_URL / DATABASE_URL")
    engine = admin_engine(url)
    try:
        print(json.dumps(seed_history(engine, args.dataset, from_step=args.from_step,
                                      until_step=args.until_step), indent=2))
    finally:
        engine.dispose()
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    uvicorn.run("riskstream.api.main:app_factory", factory=True, host=args.host, port=args.port,
                log_config=None, access_log=False, timeout_graceful_shutdown=10)
    return 0


def cmd_check_serving_parity(args: argparse.Namespace) -> int:
    from riskstream.config import AppConfig, get_settings
    from riskstream.features.registry import get_registry
    from riskstream.inference.model_loader import load_serving_model
    from riskstream.inference.predictor import ScoringService
    from riskstream.inference.serving_parity import check_serving_parity
    from riskstream.storage.database import create_db_engine

    settings, cfg = get_settings(), AppConfig.load()
    engine = create_db_engine(_require_url(settings.database_url, "DATABASE_URL"), cfg)
    registry = get_registry()
    model = load_serving_model(settings.model_bundle_path, settings.policy_path, registry, False)
    try:
        result = check_serving_parity(
            engine, model, lambda sessions: ScoringService(sessions, model, registry, cfg, "serving-parity-check"),
            args.dataset, args.start_step, args.steps, args.limit, commit=args.commit)
    finally:
        engine.dispose()
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["feature_mismatches"] == 0 else 2


def cmd_delete_scored(args: argparse.Namespace) -> int:
    from riskstream.config import get_settings
    from riskstream.storage.admin import admin_engine, delete_scored

    if not args.yes:
        raise SystemExit("Refusing to delete without --yes")
    engine = admin_engine(_require_url(get_settings().admin_database_url, "MIGRATION_DATABASE_URL"))
    try:
        print(json.dumps(delete_scored(engine, from_step=args.from_step, actor="cli"), indent=2))
    finally:
        engine.dispose()
    return 0


def cmd_history_status(args: argparse.Namespace) -> int:
    from riskstream.config import get_settings
    from riskstream.data.ingestion import load_interim
    from riskstream.storage.admin import admin_engine, history_gaps, latest_stored_step

    engine = admin_engine(_require_url(get_settings().admin_database_url, "MIGRATION_DATABASE_URL"))
    try:
        latest = latest_stored_step(engine)
        gaps = history_gaps(engine, load_interim(args.dataset), latest) if latest else []
    finally:
        engine.dispose()
    print(json.dumps({"latest_stored_step": latest, "next_step": (latest or 0) + 1,
                      "steps_differing_from_dataset": gaps[:50], "gap_count": len(gaps)}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="riskstream")
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate-synthetic", help="Write deterministic synthetic raw data")
    g.add_argument("--seed", type=int)
    g.add_argument("--num-transactions", type=int)
    g.add_argument("--num-steps", type=int)
    g.set_defaults(func=cmd_generate_synthetic)

    d = sub.add_parser("prepare-data", help="Ingest, validate, run EDA and split a dataset")
    d.add_argument("--dataset", default="paysim", choices=["paysim", "synthetic"])
    d.add_argument("--skip-eda", action="store_true")
    d.set_defaults(func=cmd_prepare_data)

    f = sub.add_parser("build-features", help="Point-in-time feature tables for each split")
    f.add_argument("--dataset", default="paysim", choices=["paysim", "synthetic"])
    f.set_defaults(func=cmd_build_features)

    t = sub.add_parser("train", help="Train, tune, calibrate and evaluate models")
    t.add_argument("--dataset", default="paysim", choices=["paysim", "synthetic"])
    t.add_argument("--models", nargs="*", help="Subset of models from configs/model.yaml")
    t.add_argument("--n-trials", type=int, help="Override tuning trials per model")
    t.add_argument("--run-name", help="MLflow parent run name")
    t.set_defaults(func=cmd_train)

    pol = sub.add_parser("tune-policy", help="Choose decision thresholds for a saved model (no retraining)")
    pol.add_argument("--dataset", default="paysim", choices=["paysim", "synthetic"])
    pol.add_argument("--bundle", help="Model bundle directory (default: newest candidate for the dataset)")
    pol.set_defaults(func=cmd_tune_policy)

    k = sub.add_parser("create-api-key", help="Generate an API key and its API_KEYS entry")
    k.add_argument("--name", required=True)
    k.add_argument("--roles", required=True, help="Comma-separated: scorer,reader,labeler")
    k.set_defaults(func=cmd_create_api_key)

    u = sub.add_parser("db-upgrade", help="Apply database migrations")
    u.add_argument("--revision", default="head")
    u.set_defaults(func=cmd_db_upgrade)

    sh = sub.add_parser("seed-history", help="Backfill historical transactions for behavioral features")
    sh.add_argument("--dataset", default="paysim", choices=["paysim", "synthetic"])
    sh.add_argument("--from-step", type=int)
    sh.add_argument("--until-step", type=int)
    sh.set_defaults(func=cmd_seed_history)

    sv = sub.add_parser("serve", help="Run the scoring API with uvicorn")
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--port", type=int, default=8000)
    sv.set_defaults(func=cmd_serve)

    sp = sub.add_parser("check-serving-parity", help="Score dataset rows through the service and compare with offline")
    sp.add_argument("--dataset", default="paysim", choices=["paysim", "synthetic"])
    sp.add_argument("--start-step", type=int, help="Default: the step after the latest stored one")
    sp.add_argument("--steps", type=int, default=1)
    sp.add_argument("--limit", type=int)
    sp.add_argument("--commit", action="store_true",
                    help="Keep the scored rows (default: roll back so the check is repeatable)")
    sp.set_defaults(func=cmd_check_serving_parity)

    ds = sub.add_parser("delete-scored", help="Delete API-scored rows at step >= N (backfill untouched)")
    ds.add_argument("--from-step", type=int, required=True)
    ds.add_argument("--yes", action="store_true", help="Required: confirms the deletion")
    ds.set_defaults(func=cmd_delete_scored)

    hs = sub.add_parser("history-status", help="Latest stored step and any gaps versus the dataset")
    hs.add_argument("--dataset", default="paysim", choices=["paysim", "synthetic"])
    hs.set_defaults(func=cmd_history_status)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging()
    ensure_directories()
    try:
        return int(args.func(args))
    except Exception:
        logger.exception("Command %s failed", args.command)
        return 1


if __name__ == "__main__":
    sys.exit(main())
