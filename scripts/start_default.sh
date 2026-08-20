#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
workspace_dir=$(cd -- "$script_dir/.." && pwd)
cd "$workspace_dir"

if [[ -f "$workspace_dir/.env.gateway.local" ]]; then
  set -a
  # Local provider credentials and the catalog encryption key are never committed.
  source "$workspace_dir/.env.gateway.local"
  set +a
fi

gateway_pid=""
cleanup() {
  if [[ -n "$gateway_pid" ]]; then
    kill "$gateway_pid" 2>/dev/null || true
    wait "$gateway_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

gateway_args=()
if [[ -n "${AI_GATEWAY_BOOTSTRAP_PASSWORD_FILE:-}" ]]; then
  gateway_args+=(--bootstrap-admin-password-file "$AI_GATEWAY_BOOTSTRAP_PASSWORD_FILE")
fi

cargo run -p vs_ai_gatewayd -- "${gateway_args[@]}" &
gateway_pid=$!

wait "$gateway_pid"
