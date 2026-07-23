#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

load_env_file() {
  local line key value
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" == \#* || "$line" != *=* ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    key="${key//[[:space:]]/}"
    if [[ -z "${!key:-}" ]]; then
      export "$key=$value"
    fi
  done < "$ENV_FILE"
}

if [[ -f "$ENV_FILE" ]]; then
  load_env_file
fi

PYTHON_BIN=""
if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python 3 is required." >&2
  exit 1
fi

DRY_RUN=false
for argument in "$@"; do
  if [[ "$argument" == "--dry-run" ]]; then
    DRY_RUN=true
    break
  fi
done

if [[ "$DRY_RUN" == false ]] && ! "$PYTHON_BIN" -c "import oracledb" >/dev/null 2>&1; then
  echo "Missing Python dependency. Run: $PYTHON_BIN -m pip install -r requirements-dev.txt" >&2
  exit 1
fi

cd "$ROOT_DIR"
exec "$PYTHON_BIN" scripts/game_day_batch.py "$@"
