import json

import polars as pl

from riskstream.data.eda import entity_activity, fraud_by_type, overview, run_eda


def test_overview_counts_match_data(small_canonical):
    df = small_canonical.collect()
    ov = overview(small_canonical)
    assert ov["rows"] == len(df)
    assert ov["fraud"] == int(df["is_fraud"].sum())
    assert ov["fraud"] + ov["legit"] == ov["rows"]


def test_fraud_by_type_sums_to_total(small_canonical):
    rows = fraud_by_type(small_canonical)
    assert sum(r["rows"] for r in rows) == overview(small_canonical)["rows"]


def test_entity_activity_single_txn_share():
    lf = pl.LazyFrame({"orig_id": ["a", "a", "b"], "dest_id": ["x", "y", "z"], "is_fraud": [0, 1, 0]})
    ea = entity_activity(lf)
    assert ea["orig_id"]["entities"] == 2
    assert ea["orig_id"]["single_txn_entities"] == 1
    assert abs(ea["orig_id"]["share_of_rows_with_repeat_entity"] - 2 / 3) < 1e-9
    assert ea["dest_id"]["single_txn_entities"] == 3


def test_run_eda_writes_reports(small_canonical, tmp_path):
    report = run_eda(small_canonical, tmp_path, "syn", True, 10_000, 0)
    assert (tmp_path / "eda_report.json").exists()
    assert "Synthetic generator output" in (tmp_path / "eda_summary.md").read_text()
    for f in report["figures"]:
        assert (tmp_path / f).stat().st_size > 0
    assert json.loads((tmp_path / "eda_report.json").read_text())["overview"]["rows"] == report["overview"]["rows"]
