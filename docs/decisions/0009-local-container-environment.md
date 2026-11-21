# ADR 0009: One application image, Compose profiles, self-healing online store

Status: accepted (Phase 5)

## Decisions

1. **One image for every Python process** (API, relay, consumers, jobs,
   MLflow). It is built from `requirements-lock.txt`, so containers and the
   venv run the same versions. `xgboost-cpu` replaces `xgboost`, avoiding
   ~300 MB of CUDA libraries on Linux. Code is copied to `/app/src` rather than
   pip-installed, because `PROJECT_ROOT` is derived from the source path.
2. **Compose profiles** (`app`, `observability`, `mlflow`) instead of one
   always-on stack. The laptop has about 1 GB of free RAM, so you start only
   what you need. Kubernetes is not used (spec: not needed before AWS).
3. **The feature consumer owns the online-store lifecycle.** On start it runs
   `online-bootstrap --if-needed`. When it sees an empty store it exits with
   code 3, and the restart policy reruns the bootstrap. A Redis lock prevents
   concurrent rebuilds. Correctness still rests on the Phase 4 design: the
   snapshot plus event-id dedup, and the API's PostgreSQL fallback.
4. **MLflow: PostgreSQL backend plus server-proxied artifacts**
   (`--serve-artifacts`), so clients on the host and in containers need no
   shared filesystem. The server's job runner is disabled: it is not used,
   and it made the container OOM.
5. **Ports bound to 127.0.0.1 only**; worker metrics are not published.
   Prometheus reaches them on the Compose network.

## Consequences

* A cold start with an empty Redis takes ~9 minutes before Redis features are
  used. Scoring works from the first minute, from PostgreSQL.
* `docker compose down -v` deletes PostgreSQL history, MLflow runs, Prometheus
  data and Grafana state.
* Redis and PostgreSQL exporters, dashboards and alert rules are Phase 6.
