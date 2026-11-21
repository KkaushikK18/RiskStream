from __future__ import annotations

from fastapi import APIRouter, Depends, Response

from riskstream.api.dependencies import get_service, raise_service_error, require_role
from riskstream.api.schemas.common import ErrorResponse
from riskstream.api.schemas.scoring import Latency, ScoreRequest, ScoreResponse
from riskstream.api.security import Principal
from riskstream.inference.predictor import ScoringService, ServiceError

router = APIRouter(prefix="/v1", tags=["scoring"])

ERRORS = {code: {"model": ErrorResponse} for code in (401, 403, 409, 413, 422, 429, 503)}


@router.post(
    "/score",
    response_model=ScoreResponse,
    status_code=201,
    responses={200: {"model": ScoreResponse, "description": "Idempotent replay of an earlier identical request"},
               **ERRORS},
    summary="Score a transaction",
)
def score(req: ScoreRequest, response: Response, principal: Principal = Depends(require_role("scorer")),
          service: ScoringService = Depends(get_service)) -> ScoreResponse:
    """Score one transaction and record the decision.

    * **201**: new transaction scored and stored.
    * **200**: the same `transaction_id` was already scored with identical content;
      the stored decision is returned (`idempotent_replay: true`). Retrying is safe.
    * **409**: the `transaction_id` exists with different content.
    * **503**: the model or database is unavailable. No decision is made: the
      service fails closed instead of approving unscored payments.
    """
    try:
        r = service.score(req, principal.name)
    except ServiceError as exc:
        raise_service_error(exc)
    response.status_code = 200 if r.idempotent_replay else 201
    response.headers["Location"] = f"/v1/transactions/{r.transaction_id}"
    return ScoreResponse(
        transaction_id=r.transaction_id, fraud_probability=r.fraud_probability, risk_level=r.risk_level,
        decision=r.decision, model_version=r.model_version, policy_version=r.policy_version,
        feature_version=r.feature_version, scored_at=r.scored_at, idempotent_replay=r.idempotent_replay,
        latency=Latency(feature_ms=round(r.feature_ms, 3), inference_ms=round(r.inference_ms, 3),
                        total_ms=round(r.total_ms, 3)),
    )
