#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Running static environment checks..."
bash "${ROOT_DIR}/scripts/check_environment.sh" --static

echo "Starting Docker services..."
docker compose up -d --build

echo "Waiting for Kafka Connect and monitoring endpoints..."
deadline=$((SECONDS + 120))
until curl --fail --silent http://localhost:8083/connectors >/dev/null \
  && curl --fail --silent http://localhost:9090/-/ready >/dev/null \
  && curl --fail --silent http://localhost:3100/ready >/dev/null \
  && curl --fail --silent http://localhost:12345/-/ready >/dev/null \
  && curl --fail --silent http://localhost:3000/api/health >/dev/null \
  && curl --fail --silent http://localhost:8090/healthz >/dev/null; do
  if ((SECONDS >= deadline)); then
    echo "Docker services did not become ready within 120 seconds." >&2
    docker compose ps
    exit 1
  fi
  sleep 3
done

echo "Environment is ready."
bash "${ROOT_DIR}/scripts/check_environment.sh"

echo "Remediation API is running at http://localhost:8090"
