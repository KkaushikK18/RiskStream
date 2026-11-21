"""Behavioral-feature primitives computed from the ``transactions`` table.

Same definitions as ``features.offline`` and ``features.online``: for a
transaction at step ``s``, window ``W`` covers the entity's transactions with
step in ``[s - W, s - 1]``; same-step rows never count. The output keys are
``features.derive.primitive_columns`` and feed the shared ``derive`` formulas.

Cost: one indexed range scan on ``(dest_id, step)`` and one on
``(orig_id, step)``, proportional to the entity's history. Phase 4 replaces
this with Redis buckets so the request path does not scan history at all.
"""

from __future__ import annotations

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from riskstream.storage.models import TransactionRow

T = TransactionRow


def _dest_query(dest_id: str, step: int, windows: tuple[int, ...]):
    cols = []
    for w in windows:
        in_w = T.step >= step - w
        cols += [
            func.sum(case((in_w, 1), else_=0)).label(f"dest_p_cnt_{w}"),
            func.sum(case((in_w, T.amount), else_=0)).label(f"dest_p_sum_{w}"),
            func.sum(case((in_w, T.log_amount), else_=0.0)).label(f"dest_p_sumlog_{w}"),
            func.sum(case((in_w, T.log_amount * T.log_amount), else_=0.0)).label(f"dest_p_sumsqlog_{w}"),
            func.sum(case((in_w & (T.type == "TRANSFER"), 1), else_=0)).label(f"dest_p_transfer_cnt_{w}"),
        ]
    cols += [func.count().label("dest_p_cnt_all"), func.max(T.step).label("dest_p_last_step")]
    return select(*cols).where(T.dest_id == dest_id, T.step < step)


def _orig_query(orig_id: str, step: int):
    return select(func.count().label("orig_p_cnt_all"), func.max(T.step).label("orig_p_last_step")).where(
        T.orig_id == orig_id, T.step < step
    )


def history_primitives(session: Session, dest_id: str, orig_id: str, step: int,
                       windows: tuple[int, ...]) -> dict[str, float | int | None]:
    dest = session.execute(_dest_query(dest_id, step, windows)).one()._mapping
    orig = session.execute(_orig_query(orig_id, step)).one()._mapping
    out: dict[str, float | int | None] = {}
    for key, value in dest.items():
        if key == "dest_p_last_step":
            out[key] = None if value is None else int(value)
        else:
            # SUM over zero rows is NULL; zero is the true value for a count/sum of no events.
            out[key] = 0.0 if value is None else float(value)
    out["orig_p_cnt_all"] = float(orig["orig_p_cnt_all"] or 0)
    out["orig_p_last_step"] = None if orig["orig_p_last_step"] is None else int(orig["orig_p_last_step"])
    return out
