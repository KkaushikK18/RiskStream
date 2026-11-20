import polars as pl
import pytest

from riskstream.data.adapters.paysim import PaySimAdapter
from riskstream.data.schema import CANONICAL_SCHEMA
from riskstream.data.validation import validate, with_rejection_reason
from tests.conftest import raw_rows, to_canonical


def test_canonical_output_has_exact_schema(small_canonical):
    assert dict(small_canonical.collect_schema()) == CANONICAL_SCHEMA


def test_transaction_ids_unique_and_ordered(small_canonical):
    df = small_canonical.collect()
    assert df["transaction_id"].n_unique() == len(df)
    assert df["event_seq"].to_list() == list(range(len(df)))


def test_dest_type_derived_from_prefix(tmp_path):
    lf = to_canonical(raw_rows([{"nameDest": "M9"}, {"nameDest": "C9", "type": "TRANSFER"}]), tmp_path)
    assert lf.collect()["dest_type"].to_list() == ["MERCHANT", "CUSTOMER"]


def test_raw_column_check_detects_missing_column(tmp_path):
    path = tmp_path / "bad.parquet"
    raw_rows([{}]).drop("isFlaggedFraud").write_parquet(path)
    problems = PaySimAdapter(path).check_raw_columns()
    assert any("missing" in p for p in problems)


def test_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        PaySimAdapter(tmp_path / "nope.csv").scan_raw()


def test_clean_data_passes(small_canonical):
    report = validate(small_canonical, "syn", max_invalid_row_rate=0.0)
    assert report.passed
    assert report.rejected_rows == 0


@pytest.mark.parametrize(
    ("row", "rule"),
    [
        ({"type": "WIRE"}, "unknown_transaction_type"),
        ({"amount": -1.0}, "negative_amount"),
        ({"amount": float("nan")}, "non_finite_numeric"),
        ({"oldbalanceOrg": -5.0}, "negative_balance"),
        ({"isFraud": 2}, "invalid_label"),
        ({"step": 0}, "invalid_step"),
        ({"nameOrig": None}, "null_required_field"),
    ],
)
def test_invalid_rows_are_rejected_with_reason(tmp_path, row, rule):
    lf = to_canonical(raw_rows([{}, row | {"step": row.get("step", 1)}]), tmp_path)
    report = validate(lf, "t", max_invalid_row_rate=1.0)
    by_name = {c.name: c for c in report.checks}
    assert by_name[rule].failed_count == 1
    reasons = with_rejection_reason(lf).collect()["_reject_reason"].to_list()
    assert reasons == [None, rule]


def test_exact_duplicate_rejected_but_first_kept(tmp_path):
    lf = to_canonical(raw_rows([{"amount": 5.0}, {"amount": 5.0}, {"amount": 6.0}]), tmp_path)
    reasons = with_rejection_reason(lf).collect()["_reject_reason"].to_list()
    assert reasons == [None, "duplicate_record", None]


def test_tolerance_exceeded_fails_dataset(tmp_path):
    lf = to_canonical(raw_rows([{}, {"amount": -1.0}]), tmp_path)
    assert not validate(lf, "t", max_invalid_row_rate=0.1).passed
    assert validate(lf, "t", max_invalid_row_rate=0.6).passed


def test_out_of_order_steps_is_structural_failure(tmp_path):
    lf = to_canonical(raw_rows([{"step": 2}, {"step": 1}]), tmp_path)
    report = validate(lf, "t", max_invalid_row_rate=1.0)
    assert not report.passed
    assert any(c.name == "temporal_order" and not c.passed for c in report.checks)


def test_wrong_dtype_is_structural_failure(small_canonical):
    lf = small_canonical.with_columns(pl.col("amount").cast(pl.String))
    report = validate(lf, "t", max_invalid_row_rate=1.0)
    assert not report.passed


def test_balance_mismatch_is_measured_not_rejected(tmp_path):
    lf = to_canonical(raw_rows([{"oldbalanceOrg": 100.0, "amount": 10.0, "newbalanceOrig": 50.0}]), tmp_path)
    report = validate(lf, "t", max_invalid_row_rate=0.0)
    by_name = {c.name: c for c in report.checks}
    assert report.passed
    assert by_name["orig_balance_mismatch"].failed_count == 1
