# Local environment (Phase 5)

Everything runs in Docker Compose on one machine. Nothing needs AWS or other
cloud credentials.

```mermaid
flowchart LR
    subgraph infra["always on"]
        PG[(postgres :5433)]
        R[(redis :6379)]
        K[(redpanda :19092)]
    end
    subgraph app["profile: app"]
        M[migrate<br/>one-shot] --> API[api :8000]
        M --> RL[relay]
        M --> FC[feature-consumer<br/>bootstrap if needed]
        API --> SW[scoring-worker]
    end
    subgraph obs["profile: observability"]
        P[prometheus :9090] --> G[grafana :3000]
    end
    subgraph ml["profile: mlflow"]
        MF[mlflow :5000]
    end
    API --- PG & R
    RL --- PG & K
    FC --- K & R
    SW --- K
    MF --- PG
    P -.scrapes.-> API & RL & FC & SW & K
```

## Prerequisites

* Docker Desktop running. The Docker VM on the development laptop has 2.8 GB.
* `.env` filled in (copy `.env.example`; every `change-me` value needs a real
  one). The compose file reads passwords, `API_KEYS`, `WORKER_API_KEY`,
  `MODEL_BUNDLE_PATH` and `POLICY_PATH` from it.
* A trained model bundle and policy under `models/`, mounted read-only into the
  containers. Produce them with `train` and `tune-policy` (README).
* For behavioral features: history loaded into PostgreSQL (`seed-history`).
  The API works without it, but every account then looks new.

The laptop has about 1 GB of free RAM. Close other applications, and start
only the profiles you need.

## Commands

```powershell
docker compose build                                   # the riskstream image (api, workers, jobs, mlflow)

docker compose --profile app up -d                     # infrastructure + application
docker compose --profile observability up -d           # + Prometheus, Grafana
docker compose --profile mlflow up -d                  # + MLflow tracking server / registry
docker compose --profile app --profile observability --profile mlflow ps

docker compose logs -f api feature-consumer            # JSON logs
docker compose --profile "*" stop                      # stop, keep containers and data
docker compose --profile "*" down                      # remove containers, keep volumes
docker compose --profile "*" down -v                   # ALSO deletes PostgreSQL, Redpanda, MLflow, Grafana data
```

Database migrations run automatically (`migrate`). To run one by hand:
`docker compose run --rm migrate`.

Host tools (replay, demo, verification commands) keep working from the venv,
because every service port is published on `127.0.0.1`. See `docs/streaming.md`.

| URL | what |
|---|---|
| http://127.0.0.1:8000/docs | API (key in `secrets/demo_api_key.txt`) |
| http://127.0.0.1:8000/health/ready | readiness incl. online features |
| http://127.0.0.1:9090/targets | Prometheus scrape targets |
| http://127.0.0.1:3000 | Grafana (user `admin`, password `GRAFANA_ADMIN_PASSWORD` in `.env`) |
| http://127.0.0.1:5000 | MLflow |

## Start-up order and self-healing

| service | starts after | on failure |
|---|---|---|
| migrate | postgres and redpanda healthy | exits non-zero, dependants do not start |
| api | migrate completed, redis healthy | restart; readiness 503 until model + database are OK |
| relay | migrate completed | restart |
| feature-consumer | migrate completed, redis healthy | runs `online-bootstrap --if-needed`, then consumes |
| scoring-worker | api healthy | restart |

**Redis restart:** Redis keeps no data on disk. When it comes back empty, the
API falls back to PostgreSQL features. The feature consumer sees
`not_bootstrapped`, exits with code 3 after committing its offsets, and is
restarted. It then rebuilds the store from PostgreSQL and resumes from the last
committed offset. A Redis lock prevents two rebuilds at once. If an operator
runs `online-bootstrap` by hand, the container waits for it instead of
starting a second rebuild.

## Container design

* One image for every Python process. It uses the same locked dependencies as
  the venv, except `xgboost-cpu` replaces `xgboost` (which pulls ~300 MB of
  CUDA libraries on Linux) and Windows-only `pywin32` is dropped.
* Runs as a non-root user (uid 10001), with `init: true` for signal handling.
  `stop_grace_period: 20s` lets consumers finish the current message and
  commit offsets on `docker compose stop`.
* Containers use service names (`postgres:5432`, `redis:6379`, `redpanda:9092`,
  `api:8000`). The host uses the published ports in `.env`.
* `LOG_FORMAT=json` in containers. Worker metrics bind to `0.0.0.0` inside the
  container, but are only reachable on the Compose network, so their ports are
  not published.
* Memory limits: `api` 768m, `feature-consumer` 1536m (the bootstrap runs
  there), the other workers 384m, `mlflow` 768m, `prometheus` and `grafana`
  384m each.
* MLflow runs with `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`. With MLflow's
  job runner enabled, the container started about 9 extra Python processes
  and was OOM-killed (exit 137) at both a 512 MB and a 1 GB limit. Without it,
  startup peaked at 461 MB.

## Measured (2026-11-21, development laptop)

| check | result |
|---|---|
| image build (first / cached) | 790 s / 16 s; image 1.73 GB; Python 3.14.10, non-root uid 10001 |
| all profiles up, steady state | ~1.05 GB total across 10 containers (api 102 MB, redis 229 MB, postgres 197 MB, mlflow 138 MB, grafana 91 MB, feature-consumer 90 MB, redpanda 81 MB, prometheus 47 MB, relay 40 MB, scoring-worker 34 MB) |
| bootstrap inside `feature-consumer` | 6.26M rows in 517 s; peak 876 MB |
| Prometheus targets | api, outbox-relay, feature-consumer, scoring-worker, redpanda, prometheus: all `up` |
| containerized pipeline, steps 610-612 | 3,890 predictions, 132,260 features vs offline: 0 mismatches |
| `/v1/score` p95 latency (Prometheus, 15 min) | 40 ms |
| scored -> applied to Redis (`riskstream_stream_event_age_seconds`) | p50 0.17 s, p95 0.92 s |
| Redis restarted empty | consumer exited (code 3), was restarted, rebuilt the store and resumed in 562 s, with no manual step. The API fell back to PostgreSQL meanwhile |
| training on the host logged to the MLflow container | run, metrics and the 4 bundle files stored in the `mlflow` database and the `mlartifacts` volume |
| tests | 188 passed |

The 1,115 step-610 transactions left over in `transactions.ingested` from an
interrupted host replay were scored as soon as the containerized worker
started. They used PostgreSQL features (`not_bootstrapped`) because the first
bootstrap was still running. That is the intended fallback, not data loss.

## MLflow

The tracking server stores runs and the model registry in the PostgreSQL
database `mlflow` (owned by `riskstream_owner`), and artifacts in the
`mlartifacts` volume. Clients upload and download artifacts through the
server (`--serve-artifacts`), so no shared filesystem is needed. `.env` sets
`MLFLOW_TRACKING_URI=http://127.0.0.1:5000`, so `train` on the host logs to
it.

Runs from before Phase 5 stay in the local `mlflow.db` / `mlruns/`. View them
with `python -m mlflow ui --backend-store-uri sqlite:///mlflow.db --port 5001`.

## Prometheus and Grafana

Prometheus scrapes the API, the three workers and Redpanda every 15 s
(`infrastructure/prometheus/prometheus.yml`) and keeps 7 days of data. Grafana
is provisioned with the Prometheus data source. Dashboards and alert rules are
Phase 6. Redis and PostgreSQL exporters are not added yet.
