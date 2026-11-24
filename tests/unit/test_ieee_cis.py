"""IEEE-CIS adapter, validation, split and features, on small hand-built data.

None of these tests needs the Kaggle files.
"""

from __future__ import annotations

import json
import math

import numpy as np
import polars as pl
import pytest

from riskstream.data import ieee_cis as data
from riskstream.data.adapters.ieee_cis import (
    ID_RAW_SCHEMA,
    TX_RAW_SCHEMA,
    V_COLUMNS,
    IEEECISAdapter,
    canonical_schema,
)
from riskstream.features import ieee_cis as feats

DAY = 86_400


def tx_rows(rows: list[dict]) -> pl.DataFrame:
    """Raw train_transaction rows; unspecified columns are null."""
    base = {c: None for c in TX_RAW_SCHEMA}
    base |= {"isFraud": 0, "TransactionAmt": 50.0, "ProductCD": "W", "card1": 1000.0, "addr1": 300.0, "D1": 0.0}
    out = []
    for i, r in enumerate(rows):
        row = base | {"TransactionID": 3_000_000 + i, "TransactionDT": DAY + i * 60} | r
        out.append(row)
    return pl.DataFrame(out, schema=TX_RAW_SCHEMA)


def id_rows(rows: list[dict]) -> pl.DataFrame:
    base = {c: None for c in ID_RAW_SCHEMA}
    return pl.DataFrame([base | r for r in rows], schema=ID_RAW_SCHEMA)


def adapter_for(tmp_path, tx: pl.DataFrame, ident: pl.DataFrame) -> IEEECISAdapter:
    tx.write_parquet(tmp_path / "tx.parquet")
    ident.write_parquet(tmp_path / "id.parquet")
    return IEEECISAdapter(tmp_path / "tx.parquet", tmp_path / "id.parquet")


# ------------------------------------------------------------------ adapter

def test_canonical_columns_and_identity_join(tmp_path):
    tx = tx_rows([{"TransactionAmt": 10.5}, {"TransactionAmt": 20.0}, {"isFraud": 1}])
    ident = id_rows([{"TransactionID": 3_000_001, "DeviceType": "mobile", "id_01": -5.0}])
    a = adapter_for(tmp_path, tx, ident)
    out = a.to_canonical(a.scan_raw()).collect()
    assert dict(out.schema) == canonical_schema()
    assert out["transaction_id"].to_list() == ["ieee_3000000", "ieee_3000001", "ieee_3000002"]
    assert out["has_identity"].to_list() == [0, 1, 0]
    assert out["DeviceType"].to_list() == [None, "mobile", None]
    assert out["day"].to_list() == [0, 0, 0] and out["amount"][0] == 10.5


def test_join_keeps_file_order_with_many_identity_rows(tmp_path):
    """Regression: without maintain_order the left join reordered rows (2,332 time-order violations)."""
    n = 5000
    tx = tx_rows([{} for _ in range(n)])
    ident = id_rows([{"TransactionID": 3_000_000 + i} for i in range(n - 1, -1, -3)])
    a = adapter_for(tmp_path, tx, ident)
    out = a.to_canonical(a.scan_raw()).collect()
    assert out["event_seq"].is_sorted() and out["event_time_s"].is_sorted()


# ------------------------------------------------------------------ validation

def test_validation_rejects_bad_rows_and_passes_clean_ones(tmp_path):
    tx = tx_rows([{}, {"TransactionAmt": 0.0}, {"TransactionAmt": -3.0}, {}, {"isFraud": 2}])
    # an exact repeat of row 0 (same time, amount, card, ...) under a new TransactionID
    dup = tx[0].with_columns(pl.lit(3_999_999, dtype=pl.Int64).alias("TransactionID"))
    tx = pl.concat([tx, dup]).sort("TransactionDT", maintain_order=True)
    a = adapter_for(tmp_path, tx, id_rows([]))
    canon = a.to_canonical(a.scan_raw())
    report = data.validate(canon, a.scan_identity(), max_invalid_row_rate=1.0)
    failed = {c.name: c.failed_count for c in report.checks if c.severity == "row" and not c.passed}
    assert failed == {"non_positive_amount": 2, "invalid_label": 1, "duplicate_record": 1}
    reasons = data.with_rejection_reason(canon).collect()["_reject_reason"].to_list()
    assert reasons.count(None) == len(reasons) - 4


def test_validation_structural_failures(tmp_path):
    tx = tx_rows([{"TransactionDT": DAY + 100}, {"TransactionDT": DAY + 50}])  # time goes backwards
    ident = id_rows([{"TransactionID": 42}])  # matches no transaction
    a = adapter_for(tmp_path, tx, ident)
    report = data.validate(a.to_canonical(a.scan_raw()), a.scan_identity(), max_invalid_row_rate=1.0)
    bad = {c.name for c in report.checks if c.severity == "structural" and not c.passed}
    assert bad == {"temporal_order", "identity_orphans"} and not report.passed


def test_split_expression_boundaries():
    days = pl.DataFrame({"day": [0, 120, 121, 150, 151, 181]})
    got = days.select(data.split_expr(120, 150).alias("s"))["s"].to_list()
    assert got == ["train", "train", "val", "val", "test", "test"]


# ------------------------------------------------------------------ behaviour features

def _base(events: list[tuple[int, float, int | None]]) -> pl.DataFrame:
    """(time_s, amount, card1 or None) -> frame with entity keys like with_entity_keys produces."""
    return pl.DataFrame({
        "transaction_id": [f"t{i}" for i in range(len(events))],
        "event_time_s": [e[0] for e in events], "amount": [e[1] for e in events],
        "_key_card1": [None if e[2] is None else e[2] for e in events],
    }, schema={"transaction_id": pl.String, "event_time_s": pl.Int64, "amount": pl.Float64, "_key_card1": pl.UInt64})


def test_behavior_windows_exclude_same_second_and_future():
    ev = [(0, 10.0, 1), (100, 20.0, 1), (100, 30.0, 1), (DAY + 50, 40.0, 1), (DAY + 200, 50.0, 1)]
    out = {r["transaction_id"]: r for r in feats.behavior_features(_base(ev), "card1", (1, 7)).iter_rows(named=True)}
    assert out["t0"]["card1_txn_count_all"] == 0 and out["t0"]["card1_hours_since_last_txn"] is None
    # t1 and t2 share second 100: neither sees the other
    assert out["t1"]["card1_txn_count_all"] == 1 and out["t2"]["card1_txn_count_all"] == 1
    # t3 at DAY+50: 1-day window [50, DAY+49] holds the two events at 100 but not the one at 0
    assert out["t3"]["card1_txn_count_1d"] == 2 and out["t3"]["card1_amount_sum_1d"] == pytest.approx(50.0)
    assert out["t3"]["card1_txn_count_7d"] == 3
    # t4 at DAY+200: 1-day window [200, DAY+199] holds only t3
    assert out["t4"]["card1_txn_count_1d"] == 1 and out["t4"]["card1_amount_sum_1d"] == pytest.approx(40.0)
    assert out["t4"]["card1_hours_since_last_txn"] == pytest.approx(150 / 3600, rel=1e-5)


def test_behavior_std_zscore_and_undefined_entity():
    ev = [(0, 10.0, 7), (10, 100.0, 7), (20, 1000.0, 7), (30, 5.0, None)]
    out = {r["transaction_id"]: r for r in feats.behavior_features(_base(ev), "card1", (1, 7)).iter_rows(named=True)}
    lg = np.log1p([10.0, 100.0])
    assert out["t2"]["card1_log_amount_std_1d"] == pytest.approx(lg.std(ddof=1), rel=1e-5)
    z = (math.log1p(1000.0) - lg.mean()) / lg.std(ddof=1)
    assert out["t2"]["card1_log_amount_zscore_1d"] == pytest.approx(z, rel=1e-5)
    assert out["t1"]["card1_log_amount_std_1d"] is None  # one prior event: std undefined
    assert all(v is None for k, v in out["t3"].items() if k != "transaction_id")  # no card1: all NaN


def test_behavior_features_ignore_labels():
    ev = [(i * 1000, float(10 + i), 1 + i % 3) for i in range(50)]
    a = feats.behavior_features(_base(ev), "card1", (1, 7))
    b = feats.behavior_features(_base(ev).with_columns(pl.lit(1).alias("is_fraud")), "card1", (1, 7))
    assert a.equals(b)


def test_uid_key_undefined_without_addr_or_d1():
    lf = pl.LazyFrame({"card1": [1.0, 1.0, 1.0, None], "addr1": [5.0, None, 5.0, 5.0],
                       "D1": [3.0, 3.0, None, 3.0], "day": [10, 10, 10, 10]},
                      schema={"card1": pl.Float32, "addr1": pl.Float32, "D1": pl.Float32, "day": pl.Int32})
    keys = feats.with_entity_keys(lf).collect()
    assert keys["_key_uid"].is_null().to_list() == [False, True, True, True]
    assert keys["_key_card1"].is_null().to_list() == [False, False, False, True]


def test_uid_same_client_on_different_days():
    """Same card holder: day - D1 (first-seen day) is constant, so the uid key matches."""
    lf = pl.LazyFrame({"card1": [1.0, 1.0], "addr1": [5.0, 5.0], "D1": [3.0, 8.0], "day": [10, 15]},
                      schema={"card1": pl.Float32, "addr1": pl.Float32, "D1": pl.Float32, "day": pl.Int32})
    k = feats.with_entity_keys(lf).collect()["_key_uid"]
    assert k[0] == k[1]


# ------------------------------------------------------------------ encoders and V selection

def _cfg(**kw) -> feats.IEEEFeatureConfig:
    base = feats.get_config()
    return feats.IEEEFeatureConfig(**({f: getattr(base, f) for f in base.__dataclass_fields__} | kw))


def test_encoders_fit_on_train_only_and_handle_unseen():
    train = pl.LazyFrame({"ProductCD": ["W"] * 30 + ["C"] * 25 + ["S"] * 2 + [None] * 21,
                          "card1": [1.0] * 78}, schema={"ProductCD": pl.String, "card1": pl.Float32})
    cfg = _cfg(categorical=("ProductCD",), frequency_numeric=("card1",), min_count=20, max_vocabulary=10)
    enc = feats.fit_encoders(train, cfg)
    assert enc["categorical"]["ProductCD"]["vocabulary"] == ["W", "C", feats.MISSING_TOKEN]  # "S" is rare
    new = pl.DataFrame({"ProductCD": ["W", "C", "S", "Z", None], "card1": [1.0, 2.0, 1.0, 1.0, None]},
                       schema={"ProductCD": pl.String, "card1": pl.Float32})
    out = new.select(feats.encoding_exprs(enc))
    assert out["ProductCD_code"].to_list() == [0.0, 1.0, 3.0, 3.0, 2.0]  # rare and unseen share code 3
    assert out["ProductCD_freq"].to_list() == pytest.approx([30 / 78, 25 / 78, 2 / 78, 0.0, 21 / 78])
    assert out["card1_freq"].to_list() == pytest.approx([1.0, 0.0, 1.0, 1.0, 0.0])


def test_encoders_reject_non_integer_ids():
    train = pl.LazyFrame({"card1": [1.5, 2.0]}, schema={"card1": pl.Float32})
    with pytest.raises(ValueError, match="integer-valued"):
        feats.fit_encoders(train, _cfg(categorical=(), frequency_numeric=("card1",)))


def test_v_selection_drops_duplicates_and_constants():
    rng = np.random.default_rng(0)
    n = 4000
    cols = {c: rng.normal(size=n).astype(np.float32) for c in V_COLUMNS}
    cols["V2"] = (cols["V1"] * 3 + 1).astype(np.float32)   # perfect duplicate of V1
    cols["V3"] = np.full(n, 7.0, np.float32)                # constant
    df = pl.DataFrame(cols).with_columns(pl.format("t{}", pl.int_range(pl.len())).alias("transaction_id"))
    sel = feats.select_v_columns(df.lazy(), _cfg(v_sample_rows=n, v_correlation_threshold=0.95))
    assert "V1" in sel["kept"] and "V2" not in sel["kept"] and "V3" not in sel["kept"]
    assert sel["dropped_correlated"]["V2"]["duplicate_of"] == "V1"
    assert sel["dropped_constant"] == ["V3"]
    assert len(sel["kept"]) == len(V_COLUMNS) - 2


# ------------------------------------------------------------------ end to end on a tiny dataset

def _tiny_splits(tmp_path) -> pl.DataFrame:
    rng = np.random.default_rng(3)
    n = 600
    tx = tx_rows([{"TransactionDT": DAY + int(i * 2.5 * 3600), "TransactionAmt": float(rng.integers(5, 500)),
                   "card1": float(rng.integers(1000, 1010)), "isFraud": int(rng.random() < 0.1),
                   "ProductCD": str(rng.choice(["W", "C"])), "V1": float(rng.normal()), "C1": 1.0,
                   "M1": str(rng.choice(["T", "F"]))} for i in range(n)])
    a = adapter_for(tmp_path, tx, id_rows([{"TransactionID": 3_000_005, "DeviceType": "mobile"}]))
    canon = a.to_canonical(a.scan_raw()).collect()
    d = tmp_path / "splits"
    d.mkdir()
    labeled = canon.with_columns(data.split_expr(40, 50).alias("split"))
    for s in data.SPLITS:
        labeled.filter(pl.col("split") == s).drop("split").write_parquet(d / f"{s}.parquet")
    return canon


def test_build_features_end_to_end(tmp_path):
    canon = _tiny_splits(tmp_path)
    out = tmp_path / "features"
    meta = feats.run_build_features(splits_dir=tmp_path / "splits", out_dir=out)
    reg = feats.IEEERegistry.load(out)
    assert sum(meta["rows"].values()) == len(canon)
    train = pl.read_parquet(out / "train.parquet")
    for fs in reg.config.feature_sets:
        assert set(reg.names(fs)) <= set(train.columns), fs
    assert train["amount"].dtype == pl.Float64  # kept exact for costs
    assert all(train[n].dtype == pl.Float32 for n in reg.names("full") if n != "amount")
    # Encoders were fitted on training rows only
    enc = json.loads((out / "encoders.json").read_text())
    assert enc["rows"] == len(train)
    # Category code / freq never in the linear set; V never in the no_vesta_v set
    assert not [n for n in reg.names("linear") if n.endswith("_code")]
    assert not [n for n in reg.names("no_vesta_v") if n.startswith("V")]
    assert not [n for n in reg.names("no_behavior") if n.startswith(("card1_txn", "uid_"))]
    assert reg.schema_hash("full") == meta["schema_hash_full"]
    # M flags map T/F to 1/0
    m = pl.read_parquet(out / "val.parquet")["M1"].unique().to_list()
    assert set(m) <= {0.0, 1.0}
