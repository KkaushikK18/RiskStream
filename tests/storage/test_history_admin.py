"""history_gaps, latest_stored_step and delete_scored (SQLite)."""

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from riskstream.api.schemas.scoring import ScoreRequest
from riskstream.config import AppConfig
from riskstream.storage.admin import delete_scored, history_gaps, latest_stored_step, seed_history, upgrade
from riskstream.storage.models import AuditEventRow, LabelRow, OutboxEventRow, PredictionRow, TransactionRow


@pytest.fixture
def engine(tmp_path):
    url = f"sqlite:///{(tmp_path / 'h.db').as_posix()}"
    upgrade(url)
    e = create_engine(url)
    yield e
    e.dispose()


def _count(engine, model, *where):
    with Session(engine) as s:
        return s.execute(select(func.count()).select_from(model).where(*where)).scalar_one()


def test_gaps_detect_missing_and_extra_steps(engine, small_canonical):
    seed_history(engine, "synthetic", canonical=small_canonical, until_step=20)
    assert latest_stored_step(engine) == 20
    assert history_gaps(engine, small_canonical, 20) == []
    assert history_gaps(engine, small_canonical, 22) == [21, 22]  # dataset has them, database does not
    seed_history(engine, "synthetic", canonical=small_canonical, from_step=25, until_step=25)
    assert history_gaps(engine, small_canonical, 25) == [21, 22, 23, 24]


def test_delete_scored_removes_only_api_rows(engine, small_canonical, artifacts):
    from riskstream.features.registry import get_registry
    from riskstream.inference.model_loader import load_serving_model
    from riskstream.inference.predictor import ScoringService
    from riskstream.storage.database import session_factory
    from tests.conftest import txn

    seed_history(engine, "synthetic", canonical=small_canonical, until_step=20)
    history = _count(engine, TransactionRow)
    model = load_serving_model(artifacts["bundle"], artifacts["policy"], get_registry(), False)
    service = ScoringService(session_factory(engine), model, get_registry(), AppConfig.load(), "t")
    for i, step in enumerate([21, 22, 30]):
        service.score(ScoreRequest(**txn(f"api-{i}", step)), "t")
    from riskstream.api.schemas.scoring import LabelRequest
    from tests.conftest import ts

    service.record_label(LabelRequest(transaction_id="api-1", is_fraud=True, source="simulation",
                                      label_time=ts(23)), "t")

    counts = delete_scored(engine, from_step=22, actor="test")
    assert counts == {"transactions": 2, "predictions": 2, "labels": 1, "outbox_events": 3}
    assert _count(engine, TransactionRow) == history + 1  # api-0 at step 21 stays
    assert _count(engine, PredictionRow) == 1 and _count(engine, LabelRow) == 0
    assert _count(engine, OutboxEventRow) == 1
    assert _count(engine, AuditEventRow, AuditEventRow.event_type == "admin_delete_scored") == 1
    # backfilled history at steps >= 22 would never be deleted
    assert delete_scored(engine, from_step=1, actor="test")["transactions"] == 1
    assert _count(engine, TransactionRow) == history
