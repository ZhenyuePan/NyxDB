#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
PROTOC_BIN=/tmp/protoc/bin
GOPATH_DIR=${GOPATH:-$HOME/go}
export PATH="$PROTOC_BIN:$GOPATH_DIR/bin:$PATH"
cd "$REPO_ROOT"
protoc -I api \
  --go_out=pkg/api --go_opt=paths=source_relative \
  --go-grpc_out=pkg/api --go-grpc_opt=paths=source_relative \
  api/common.proto \
  api/kv.proto \
  api/admin.proto \
  api/pd.proto \
  api/raft.proto
