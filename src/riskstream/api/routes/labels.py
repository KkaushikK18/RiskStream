from __future__ import annotations

from fastapi import APIRouter, Depends, Response

from riskstream.api.dependencies import get_service, raise_service_error, require_role
from riskstream.api.schemas.common import ErrorResponse
from riskstream.api.schemas.scoring import LabelRequest, LabelResponse
from riskstream.api.security import Principal
from riskstream.inference.predictor import ScoringService, ServiceError
from riskstream.storage.database import utc

router = APIRouter(prefix="/v1", tags=["labels"])
ERRORS = {code: {"model": ErrorResponse} for code in (401, 403, 404, 409, 413, 422, 429, 503)}


@router.post("/labels", response_model=LabelResponse, status_code=201,
             responses={200: {"model": LabelResponse, "description": "Identical label already recorded"}, **ERRORS},
             summary="Record the known outcome of a scored transaction")
def record_label(req: LabelRequest, response: Response, principal: Principal = Depends(require_role("labeler")),
                 service: ScoringService = Depends(get_service)) -> LabelResponse:
    """Attach the eventual outcome (fraud or not) to a transaction.

    * **201**: recorded, with `available_at` = time of receipt (used for delayed-label analysis).
    * **200**: the identical label was already recorded.
    * **404**: the transaction does not exist.
    * **409**: a different label exists. Labels are never overwritten.
    * **422**: `label_time` is before the transaction or in the future.
    """
    try:
        r = service.record_label(req, principal.name)
    except ServiceError as exc:
        raise_service_error(exc)
    response.status_code = 200 if r.idempotent_replay else 201
    row = r.row
    return LabelResponse(transaction_id=row.transaction_id, is_fraud=row.is_fraud, source=row.source,
                         label_time=utc(row.label_time), available_at=utc(row.available_at),
                         idempotent_replay=r.idempotent_replay)
