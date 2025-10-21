#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TRACE_OUT="${REPO_ROOT}/test_result/tracing_example_spans.txt"
TMP_ENGINE_DIR="/tmp/nyxdb-trace"
rm -rf "${TMP_ENGINE_DIR}" >/dev/null 2>&1 || true

for port in 27001 27002; do
  pids=$(lsof -ti :${port} 2>/dev/null || true)
  if [[ -n "${pids}" ]]; then
    kill ${pids} >/dev/null 2>&1 || true
    wait ${pids} 2>/dev/null || true
  fi
done

CONFIG_DIR="$(mktemp -d)"
CONFIG_FILE="${CONFIG_DIR}/server.yaml"
cat >"${CONFIG_FILE}" <<'CONF'
nodeID: 1
engine:
  dir: /tmp/nyxdb-trace
  enableDiagnostics: false
cluster:
  clusterMode: true
  nodeAddress: 127.0.0.1:27001
  clusterAddresses:
    - 1@127.0.0.1:27001
grpc:
  address: 127.0.0.1:27002
pd:
  address: ""
  heartbeatInterval: 2s
observability:
  metricsAddress: ""
  tracing:
    endpoint: ""
    insecure: true
    serviceName: nyxdb-tracing-demo
    sampleRatio: 1.0
CONF

export OTEL_TRACES_EXPORTER=console
export OTEL_EXPORTER_CONSOLE_VERBOSITY=detailed
export OTEL_RESOURCE_ATTRIBUTES="service.name=nyxdb-tracing-demo"

SERVER_LOG="${CONFIG_DIR}/server.log"

go run ./cmd/nyxdb-server -config "${CONFIG_FILE}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!
cleanup() {
  kill ${SERVER_PID} >/dev/null 2>&1 || true
  wait ${SERVER_PID} 2>/dev/null || true
  rm -rf "${CONFIG_DIR}" "${TMP_ENGINE_DIR}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sleep 3

go run ./cmd/nyxdb-cli kv put --addr 127.0.0.1:27002 --key trace-key --value trace-value >/dev/null
sleep 1

kill ${SERVER_PID} >/dev/null 2>&1 || true
wait ${SERVER_PID} 2>/dev/null || true

if [[ -s "${SERVER_LOG}" ]]; then
	cp "${SERVER_LOG}" "${TRACE_OUT}"
else
	echo "No traces captured" > "${TRACE_OUT}"
fi
