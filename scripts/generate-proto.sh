#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
api_root="${MYCEL_API_ROOT:-${repo_root}/../mycel-api}"

if [[ ! -d "${api_root}/api/proto" ]]; then
  echo "mycel-api proto root not found: ${api_root}/api/proto" >&2
  echo "Set MYCEL_API_ROOT to a checkout of github.com/myceldb/mycel-api." >&2
  exit 1
fi

tools_dir="${repo_root}/.cache/proto-tools/bin"
mkdir -p "${tools_dir}"
GOBIN="${tools_dir}" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
GOBIN="${tools_dir}" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
export PATH="${tools_dir}:${PATH}"

rm -rf "${repo_root}/internal/gen"
(
  cd "${repo_root}"
  go run github.com/bufbuild/buf/cmd/buf@v1.50.1 generate "${api_root}" --template buf.gen.yaml
  if [[ -d "${repo_root}/internal/clustering/proto" ]]; then
    go run github.com/bufbuild/buf/cmd/buf@v1.50.1 generate "${repo_root}/internal/clustering/proto" --template buf.gen.yaml
  fi
)
"${repo_root}/scripts/write-gen-compat.sh"
