#!/usr/bin/env bash
set -euo pipefail

fail() { printf '%s\n' "$*" >&2; exit 1; }

[[ $# == 4 ]] || fail 'usage: promoteAcceptedDockerImage.sh <stable-version> <accepted-index-ref> <daemon-sha> <context-sha256>'
version=$1
accepted=$2
revision=$3
context_digest=$4
repo=docker.io/myceldb/mycel
[[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail 'expected a stable version tag'
[[ "$accepted" =~ ^docker\.io/myceldb/mycel@sha256:[a-f0-9]{64}$ ]] || fail "expected $repo pinned by digest"
[[ "$revision" =~ ^[a-f0-9]{40}$ && "$context_digest" =~ ^[a-f0-9]{64}$ ]] || fail 'invalid build provenance'
digest=${accepted##*@sha256:}
target=$repo:$version
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

file_sha256() { sha256sum "$1" | awk '{print $1}'; }

docker buildx imagetools inspect --raw "$accepted" > "$scratch/index"
[[ $(file_sha256 "$scratch/index") == "$digest" ]] || fail 'accepted index does not match requested digest'
jq -e '[.manifests[] | .platform | select(.os != "unknown" or .architecture != "unknown") | .os + "/" + .architecture] | sort == ["linux/amd64", "linux/arm64"]' \
  "$scratch/index" >/dev/null || fail 'accepted index must contain exactly linux/amd64 and linux/arm64 runnable manifests'
docker buildx imagetools inspect --format '{{json .Image}}' "$accepted" | jq -e \
  --arg revision "$revision" --arg context "$context_digest" '
    . as $images | all(["amd64", "arm64"][];
      . as $arch | $images["linux/" + $arch]
      | .os == "linux" and .architecture == $arch
      and .config.Labels["org.opencontainers.image.source"] == "https://github.com/MycelDB/mycel"
      and .config.Labels["org.opencontainers.image.revision"] == $revision
      and .config.Labels["io.myceldb.source.dirty"] == "false"
      and .config.Labels["io.myceldb.build.context-digest"] == $context
      and (.config.Labels["io.myceldb.build.command"] | type == "string" and length > 0))' \
  >/dev/null || fail 'accepted image provenance does not match the clean daemon and pinned API'

if docker buildx imagetools inspect --raw "$target" > "$scratch/current" 2> "$scratch/lookup-error"; then
  [[ $(file_sha256 "$scratch/current") == "$digest" ]] || fail "refusing to replace existing stable tag $target"
  printf '%s already resolves to sha256:%s\n' "$target" "$digest"
  exit 0
fi
if ! grep -Fxq "ERROR: $target: not found" "$scratch/lookup-error"; then
  fail "could not prove $target is absent; not publishing"
fi

# A single source index is copied byte-for-byte. Check the planned bytes before
# writing and independently hash the registry's response after publication.
docker buildx imagetools create --dry-run --prefer-index=false "$accepted" > "$scratch/planned"
cmp -s "$scratch/planned" "$scratch/index" || {
  printf '\n' >> "$scratch/index" # Buildx dry-run prints a terminating newline.
  cmp -s "$scratch/planned" "$scratch/index" || fail 'promotion would rewrite the accepted index'
}
docker buildx imagetools create --prefer-index=false -t "$target" "$accepted"
docker buildx imagetools inspect --raw "$target" > "$scratch/current"
[[ $(file_sha256 "$scratch/current") == "$digest" ]] || fail "published tag $target does not resolve to accepted digest; investigate without overwriting it"
printf '%s resolves to sha256:%s\n' "$target" "$digest"
