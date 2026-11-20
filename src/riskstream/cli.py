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
