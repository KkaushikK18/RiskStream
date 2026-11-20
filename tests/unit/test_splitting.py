import polars as pl
import pytest

from riskstream.data.splitting import SplitBoundaries, assign_split, split_summary, write_splits


def test_boundaries_must_increase():
    with pytest.raises(ValueError):
        SplitBoundaries(10, 10)
    with pytest.raises(ValueError):
        SplitBoundaries(0, 5)


def test_steps_assigned_by_inclusive_bounds():
    lf = pl.LazyFrame({"step": [1, 10, 11, 20, 21, 30]})
    out = assign_split(lf, SplitBoundaries(10, 20)).collect()["split"].to_list()
    assert out == ["train", "train", "val", "val", "test", "test"]


def test_splits_are_disjoint_and_ordered_in_time(small_canonical, tmp_path):
    bounds = SplitBoundaries(30, 40)
    paths = write_splits(small_canonical, bounds, tmp_path)
    tr, va, te = (pl.read_parquet(paths[s]) for s in ("train", "val", "test"))
    assert tr["step"].max() <= 30 < va["step"].min()
    assert va["step"].max() <= 40 < te["step"].min()
    ids = [set(d["transaction_id"]) for d in (tr, va, te)]
    assert not (ids[0] & ids[1]) and not (ids[1] & ids[2]) and not (ids[0] & ids[2])
    assert len(tr) + len(va) + len(te) == small_canonical.select(pl.len()).collect().item()


def test_class_distribution_not_altered(small_canonical, tmp_path):
    bounds = SplitBoundaries(30, 40)
    paths = write_splits(small_canonical, bounds, tmp_path)
    total_fraud = small_canonical.select(pl.col("is_fraud").sum()).collect().item()
    split_fraud = sum(pl.read_parquet(p)["is_fraud"].sum() for p in paths.values())
    assert split_fraud == total_fraud


def test_summary_counts_consistent(small_canonical):
    s = split_summary(small_canonical, SplitBoundaries(30, 40))
    assert sum(v["rows"] for v in s["splits"].values()) == s["total_rows"]
    assert abs(sum(v["fraud_share"] for v in s["splits"].values()) - 1.0) < 1e-9
    assert 0.0 <= s["dest_entity_overlap"]["val_dest_seen_in_train_share"] <= 1.0
