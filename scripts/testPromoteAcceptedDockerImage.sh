#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
export FIXTURE=$scratch
export PATH="$scratch:$PATH"
revision=$(printf '%040d' 1)
context=$(printf '%064d' 2)
printf '%s' '{"manifests":[{"platform":{"os":"linux","architecture":"amd64"}},{"platform":{"os":"linux","architecture":"arm64"}}]}' > "$scratch/index"
digest=$(sha256sum "$scratch/index" | awk '{print $1}')
export ACCEPTED=docker.io/myceldb/mycel@sha256:$digest
export TARGET=docker.io/myceldb/mycel:v0.26.0
jq -n --arg revision "$revision" --arg context "$context" '
  def image($arch): {os:"linux",architecture:$arch,config:{Labels:{
    "org.opencontainers.image.source":"https://github.com/MycelDB/mycel",
    "org.opencontainers.image.revision":$revision,"io.myceldb.source.dirty":"false",
    "io.myceldb.build.command":"test","io.myceldb.build.context-digest":$context}}};
  {"linux/amd64":image("amd64"),"linux/arm64":image("arm64")}' > "$scratch/labels"

cat > "$scratch/docker" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1 $2" == 'buildx imagetools' ]]
shift 2
if [[ "$*" == "inspect --raw $ACCEPTED" ]]; then
  cat "$FIXTURE/index"
elif [[ "$*" == "inspect --format {{json .Image}} $ACCEPTED" ]]; then
  cat "$FIXTURE/labels"
elif [[ "$*" == "inspect --raw $TARGET" ]]; then
  if [[ -f "$FIXTURE/target" ]]; then
    cat "$FIXTURE/target"
  elif [[ ${MODE:-} == unavailable ]]; then
    echo 'connection failed' >&2
    exit 1
  else
    echo "ERROR: $TARGET: not found" >&2
    exit 1
  fi
elif [[ "$*" == "create --dry-run --prefer-index=false $ACCEPTED" ]]; then
  cat "$FIXTURE/index"
  printf '\n'
  [[ ${MODE:-} != rewrite ]] || printf 'changed'
elif [[ "$*" == "create --prefer-index=false -t $TARGET $ACCEPTED" ]]; then
  printf 'write\n' >> "$FIXTURE/writes"
  cp "$FIXTURE/index" "$FIXTURE/target"
else
  printf 'unexpected docker command: %s\n' "$*" >&2
  exit 1
fi
MOCK
chmod +x "$scratch/docker"

promote() { bash "$script_dir/promoteAcceptedDockerImage.sh" v0.26.0 "$ACCEPTED" "${1:-$revision}" "$context"; }
reject() {
  expected=$1
  shift
  if "$@" > "$scratch/attempt" 2>&1; then
    echo "unexpected successful promotion: $expected" >&2
    exit 1
  fi
  grep -Fq "$expected" "$scratch/attempt"
}
reject 'provenance does not match' promote "$(printf '%040d' 3)"
export MODE=unavailable
reject 'could not prove' promote
export MODE=rewrite
reject 'would rewrite' promote
unset MODE
test ! -e "$scratch/writes"
promote
promote # Same digest is idempotent.
printf 'different' > "$scratch/target"
reject 'refusing to replace existing stable tag' promote
[[ $(wc -l < "$scratch/writes") -eq 1 ]]
printf '%s\n' 'Accepted Docker image promotion checks passed'
