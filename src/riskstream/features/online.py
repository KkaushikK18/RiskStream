"""Online (incremental) primitives with the same semantics as ``offline``.

``EntityHistory`` keeps, per entity, one bucket of sufficient statistics per
step that had events. A query for a transaction at step ``s`` sums buckets with
step in ``[s - W, s - 1]``; buckets at step ``s`` are ignored, matching the
offline rule. Buckets older than the longest window are pruned, while all-time
counts and the last step are kept as running values.

This in-memory store is the reference implementation of the semantics. Phase 4
moves the same bucket layout into Redis (one hash per entity and hour bucket,
updated with atomic HINCRBYFLOAT, expiring after the longest window).
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field

import polars as pl

from riskstream.features.derive import derive_features, primitive_columns, window_primitive_names


@dataclass
class _Bucket:
    cnt: float = 0.0
    sum: float = 0.0
    sumlog: float = 0.0
    sumsqlog: float = 0.0
    transfer_cnt: float = 0.0


@dataclass
class _Entity:
    buckets: dict[int, _Bucket] = field(default_factory=dict)
    cnt_by_step: dict[int, float] = field(default_factory=dict)  # for all-time counts
    cnt_before_oldest: float = 0.0


class EntityHistory:
    def __init__(self, windows: tuple[int, ...]) -> None:
        self.windows = tuple(sorted(windows))
        self.max_window = self.windows[-1]
        self._dest: dict[str, _Entity] = defaultdict(_Entity)
        self._orig: dict[str, _Entity] = defaultdict(_Entity)
        self._seen_ids: set[str] = set()

    # ------------------------------------------------------------- queries

    @staticmethod
    def _all_before(e: _Entity, step: int) -> tuple[float, int | None]:
        cnt = e.cnt_before_oldest
        last = None
        for st, c in e.cnt_by_step.items():
            if st < step:
                cnt += c
                last = st if last is None or st > last else last
        return cnt, last

    def primitives(self, dest_id: str, orig_id: str, step: int) -> dict[str, float | int | None]:
        out: dict[str, float | int | None] = {}
        d = self._dest.get(dest_id) or _Entity()
        for w in self.windows:
            agg = _Bucket()
            for st, b in d.buckets.items():
                if step - w <= st <= step - 1:
                    agg.cnt += b.cnt
                    agg.sum += b.sum
                    agg.sumlog += b.sumlog
                    agg.sumsqlog += b.sumsqlog
                    agg.transfer_cnt += b.transfer_cnt
            names = window_primitive_names("dest", w)
            for name, val in zip(names, (agg.cnt, agg.sum, agg.sumlog, agg.sumsqlog, agg.transfer_cnt),
                                 strict=True):
                out[name] = val
        out["dest_p_cnt_all"], out["dest_p_last_step"] = self._all_before(d, step)
        o = self._orig.get(orig_id) or _Entity()
        out["orig_p_cnt_all"], out["orig_p_last_step"] = self._all_before(o, step)
        return out

    # ------------------------------------------------------------- updates

    def update(self, transaction_id: str, dest_id: str, orig_id: str, step: int,
               amount: float, txn_type: str) -> bool:
        """Apply one event. Returns False (and changes nothing) for a repeat id."""
        if transaction_id in self._seen_ids:
            return False
        self._seen_ids.add(transaction_id)
        log_amt = math.log1p(amount)
        d = self._dest[dest_id]
        b = d.buckets.setdefault(step, _Bucket())
        b.cnt += 1
        b.sum += amount
        b.sumlog += log_amt
        b.sumsqlog += log_amt * log_amt
        b.transfer_cnt += 1.0 if txn_type == "TRANSFER" else 0.0
        d.cnt_by_step[step] = d.cnt_by_step.get(step, 0.0) + 1
        o = self._orig[orig_id]
        o.cnt_by_step[step] = o.cnt_by_step.get(step, 0.0) + 1
        self._prune(d, step)
        return True

    def _prune(self, e: _Entity, current_step: int) -> None:
        # Window buckets older than the longest window can never be queried again
        # (queries only move forward in time in a live stream).
        horizon = current_step - self.max_window - 1
        for st in [st for st in e.buckets if st < horizon]:
            del e.buckets[st]
        # Fold all-time counts older than the horizon into one number, keeping
        # the latest folded step so "hours since last" stays exact.
        old = [st for st in e.cnt_by_step if st < horizon]
        if len(old) > 1:
            latest = max(old)
            for st in old:
                if st != latest:
                    e.cnt_before_oldest += e.cnt_by_step.pop(st)


TXN_FIELDS: tuple[str, ...] = ("step", "type", "amount", "dest_type", "orig_balance_before")


def features_from_primitives(txn: dict, prims: dict, feature_names: list[str],
                             windows: tuple[int, ...]) -> dict[str, float]:
    """Apply the shared ``derive`` formulas to one transaction and its primitives.

    Every online primitive source (in-memory history, PostgreSQL, Redis) ends
    here, so no feature formula is implemented outside ``features.derive``.
    """
    missing = [c for c in primitive_columns(windows) if c not in prims]
    if missing:
        raise KeyError(f"Missing primitives: {missing}")
    row = {k: txn[k] for k in TXN_FIELDS} | {c: prims[c] for c in primitive_columns(windows)}
    schema = {
        "step": pl.Int32, "type": pl.String, "amount": pl.Float64, "dest_type": pl.String,
        "orig_balance_before": pl.Float64,
        **{c: (pl.Int32 if c.endswith("last_step") else pl.Float64) for c in primitive_columns(windows)},
    }
    df = pl.DataFrame([row], schema=schema)
    return derive_features(df, feature_names, windows).row(0, named=True)


def online_features(history: EntityHistory, txn: dict, feature_names: list[str]) -> dict[str, float]:
    """Features for one transaction from current history (call BEFORE ``update``)."""
    prims = history.primitives(txn["dest_id"], txn["orig_id"], int(txn["step"]))
    return features_from_primitives(txn, prims, feature_names, history.windows)
