# Scoring API

Interactive documentation is served by the running service at
`http://127.0.0.1:8000/docs` (Swagger UI) and `/openapi.json`.

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API (FastAPI)
    participant P as PostgreSQL
    C->>A: POST /v1/score (X-API-Key)
    A->>A: request id, size limit, load shedding, auth, rate limit, validation
    A->>P: transaction_id already stored?
    alt identical request already scored
        P-->>A: stored prediction
        A-->>C: 200 (idempotent_replay)
    else new
        A->>P: history primitives for dest / orig (earlier hours)
        A->>A: derive features -> model -> calibrator -> policy
        A->>P: BEGIN; insert transaction, prediction, outbox event; COMMIT
        A-->>C: 201 decision
    end
```

## Authentication

Header `X-API-Key: <key>`. Create a key:

```powershell
python -m riskstream.cli create-api-key --name demo --roles scorer,reader,labeler
```

The command prints the key once, and the JSON entry for `API_KEYS` in `.env`.

| role | endpoints |
|---|---|
| scorer | `POST /v1/score` |
| reader | `GET /v1/transactions/{id}`, `GET /v1/predictions`, `GET /v1/model` |
| labeler | `POST /v1/labels` |

## Endpoints

| method and path | auth | purpose |
|---|---|---|
| `GET /health/live` | none | process is running |
| `GET /health/ready` | none | model loaded and database reachable; 503 otherwise |
| `GET /metrics` | none | Prometheus metrics |
| `POST /v1/score` | scorer | score and store a transaction |
| `GET /v1/transactions/{transaction_id}` | reader | stored transaction (masked), prediction, label |
| `GET /v1/predictions?decision=REVIEW&limit=50&cursor=...` | reader | newest first, keyset pagination |
| `GET /v1/model` | reader | loaded model and policy |
| `POST /v1/labels` | labeler | attach the known outcome |

### `POST /v1/score`

```json
{
  "transaction_id": "txn_10001",
  "timestamp": "2026-01-25T10:30:00Z",
  "type": "TRANSFER",
  "amount": 4500.0,
  "orig_account_id": "C1231006815",
  "orig_balance_before": 4500.0,
  "dest_account_id": "C553264065",
  "dest_type": "CUSTOMER"
}
```

Response shape (values depend on the model and history):

```json
{
  "transaction_id": "txn_10001",
  "fraud_probability": 0.0,
  "risk_level": "LOW",
  "decision": "APPROVE",
  "model_version": "<model version>",
  "policy_version": "2.0.0",
  "feature_version": "1.0.0",
  "scored_at": "...",
  "idempotent_replay": false,
  "latency": {"feature_ms": 0.0, "inference_ms": 0.0, "total_ms": 0.0}
}
```

* `timestamp` must include a timezone. It maps to PaySim's hourly step:
  `step = floor((timestamp - 2026-01-01T00:00Z) / 1 h) + 1`.
* Mapping from PaySim columns: `nameOrig` -> `orig_account_id`,
  `oldbalanceOrg` -> `orig_balance_before`, `nameDest` -> `dest_account_id`,
  destination id prefix `M`/`C` -> `dest_type`. Post-transaction balances and
  destination balances are not accepted.
* Amounts and balances are rounded to 2 decimals before features are computed
  and stored.

| status | meaning |
|---|---|
| 201 | scored and stored |
| 200 | identical request already scored; stored result returned |
| 401 / 403 | missing or invalid key / key lacks the role |
| 409 | `transaction_id` exists with different content |
| 413 | body larger than 16 KB |
| 422 | validation failed (field locations listed, values not echoed) |
| 429 | rate limit; see `Retry-After` |
| 503 | model or database unavailable, or server at capacity. No decision was made |

### `POST /v1/labels`

```json
{"transaction_id": "txn_10001", "is_fraud": true, "source": "chargeback", "label_time": "2026-01-27T09:00:00Z"}
```

`source` is one of `chargeback`, `analyst_review`, `customer_report` or
`simulation`. The server records `available_at` (time of receipt) for
delayed-label analysis.

| status | meaning |
|---|---|
| 201 | recorded |
| 200 | identical label already recorded |
| 404 | unknown transaction |
| 409 | a different label exists; labels are never overwritten |
| 422 | `label_time` before the transaction or in the future |

## Errors

Every error uses the same envelope:

```json
{"error": {"code": "transaction_id_conflict", "message": "...", "request_id": "3f2c...", "details": null}}
```

Unexpected errors return `internal_error` with no internal detail. Quote the
`request_id` to find the full error in the logs.

## Headers

* `X-Request-ID`: echoed if supplied and safe (`[A-Za-z0-9._:-]{1,128}`), otherwise generated.
* `X-Correlation-ID`: echoed if supplied, otherwise equal to the request id.
* `Location` on scoring responses; `Retry-After` on 429 and 503.

## Metrics

| metric | type | labels |
|---|---|---|
| `riskstream_http_requests_total` | counter | method, route (template), status |
| `riskstream_http_request_duration_seconds` | histogram | method, route |
| `riskstream_score_stage_seconds` | histogram | stage: idempotency_check, history_query, feature_derivation, model_inference, persist |
| `riskstream_decisions_total` | counter | decision, model_version |
| `riskstream_fraud_probability` | histogram | model_version |
| `riskstream_http_inflight_requests`, `riskstream_load_shed_total` | gauge, counter | |
| `riskstream_rate_limited_total`, `riskstream_auth_failures_total` | counter | client / reason |
| `riskstream_idempotent_replays_total`, `riskstream_conflicts_total` | counter | endpoint |
| `riskstream_labels_total` | counter | source, is_fraud |
| `riskstream_dependency_errors_total` | counter | dependency |
| `riskstream_db_pool_checked_out`, `riskstream_db_pool_size` | gauge | |
| `riskstream_model_loaded`, `riskstream_model_info` | gauge | versions |

Design decisions: [ADR 0007](decisions/0007-inference-service-design.md).
