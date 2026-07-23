#!/usr/bin/env bash

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
MODE="${1:-all}"
ERRORS=0

ok() {
  printf '[OK]   %s\n' "$1"
}

warn() {
  printf '[WARN] %s\n' "$1"
}

fail() {
  printf '[FAIL] %s\n' "$1" >&2
  ERRORS=$((ERRORS + 1))
}

require_command() {
  if command -v "$1" >/dev/null 2>&1; then
    ok "Command available: $1"
  else
    fail "Missing command: $1"
  fi
}

check_url() {
  local name="$1"
  local url="$2"

  if curl --fail --silent --show-error --max-time 5 "$url" >/dev/null; then
    ok "$name: $url"
  else
    fail "$name is not ready: $url"
  fi
}

load_env_file() {
  local line key value

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" == \#* || "$line" != *=* ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    key="${key//[[:space:]]/}"
    export "$key=$value"
  done < "$ENV_FILE"
}

printf 'Checking project at %s\n' "$ROOT_DIR"

require_command docker
require_command curl

if command -v python3 >/dev/null 2>&1; then
  ok "Command available: python3"
elif command -v python >/dev/null 2>&1; then
  ok "Command available: python"
else
  warn "Python 3 is unavailable; only local tests and game-day script need it"
fi

if [[ ! -f "$ENV_FILE" ]]; then
  fail "Missing .env; copy .env.example to .env and fill in the credentials"
else
  load_env_file
  ok ".env loaded"

  for variable in \
    ORACLE_DSN \
    ORACLE_USER \
    ORACLE_PASSWORD \
    KAFKA_BOOTSTRAP_SERVERS \
    KAFKA_CONNECT_URL; do
    if [[ -n "${!variable:-}" ]]; then
      ok "$variable is set"
    else
      fail "$variable is missing or blank"
    fi
  done

  if [[ "${ORACLE_USER:-}" == "SYS" ]]; then
    fail "ORACLE_USER=SYS is not supported; use a dedicated remediation user"
  fi
fi

if command -v docker >/dev/null 2>&1; then
  if docker info >/dev/null 2>&1; then
    ok "Docker daemon is reachable"
  else
    fail "Docker daemon is not reachable"
  fi

  if docker compose version >/dev/null 2>&1; then
    ok "Docker Compose is available"
    if (cd "$ROOT_DIR" && docker compose config --quiet); then
      ok "docker-compose.yml is valid"
    else
      fail "docker-compose.yml is invalid"
    fi
  else
    fail "Docker Compose plugin is unavailable"
  fi
fi

if [[ "$MODE" != "--static" ]]; then
  check_url "Kafka Connect" "http://localhost:8083/connectors"
  check_url "Prometheus" "http://localhost:9090/-/ready"
  check_url "Loki" "http://localhost:3100/ready"
  check_url "Alloy" "http://localhost:12345/-/ready"
  check_url "Grafana" "http://localhost:3000/api/health"
  check_url "Kafka UI" "http://localhost:8080/"
  check_url "Remediation API" "http://localhost:8090/healthz"

fi

if ((ERRORS > 0)); then
  printf '\nEnvironment check failed with %d error(s).\n' "$ERRORS" >&2
  exit 1
fi

printf '\nEnvironment check passed.\n'
