# syntax=docker/dockerfile:1
# RiskStream application image: API, streaming workers, CLI jobs and the MLflow server.
#   docker compose build
#
# Dependencies come from requirements-lock.txt (exact versions), with two Linux
# adjustments: pywin32 is Windows-only and is skipped; xgboost is replaced by
# xgboost-cpu of the same version, which avoids the CUDA/NCCL wheels (~300 MB)
# that the Linux xgboost package pulls in.

FROM python:3.14-slim AS builder
# Long timeouts and many retries: PyPI downloads from the build network can be slow.
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=10
COPY requirements-lock.txt /tmp/requirements-lock.txt
# The cache mount keeps downloaded wheels between builds (not part of the image).
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m venv /opt/venv \
 && sed -e '1s/^\xEF\xBB\xBF//' -e 's/\r$//' /tmp/requirements-lock.txt \
    | grep -viE '^(pywin32)==' \
    | sed -E 's/^xgboost==/xgboost-cpu==/' > /tmp/requirements.txt \
 && /opt/venv/bin/pip install --only-binary=:all: -r /tmp/requirements.txt

FROM python:3.14-slim
# libgomp1: OpenMP runtime needed by LightGBM and XGBoost
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgomp1 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10001 app && useradd --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app

COPY --from=builder /opt/venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONIOENCODING=utf-8 \
    MLFLOW_DISABLE_AGENT_HINT=1

WORKDIR /app
# The code is not pip-installed: PROJECT_ROOT is derived from the source path (/app).
COPY alembic.ini pyproject.toml ./
COPY configs ./configs
COPY src ./src
# Writable directories for the non-root user. models/ is mounted read-only at run time.
RUN mkdir -p logs data/raw data/interim data/processed data/synthetic reports/eda reports/model_evaluation \
             models /mlartifacts \
 && chown -R app:app logs data reports /mlartifacts

USER app
EXPOSE 8000
CMD ["python", "-m", "riskstream.cli", "serve", "--host", "0.0.0.0", "--port", "8000"]
