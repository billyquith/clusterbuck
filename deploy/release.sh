#!/usr/bin/env bash
# Cut a worker release: bump, test, tag, let CI build it, and optionally publish it to a
# coordinator. The one command that replaces the hand-edited release steps.
#
#   deploy/release.sh 0.22.0
#   deploy/release.sh 0.22.0 --publish-via my-coordinator     # an ssh host
#
# The build is made ONCE, by the release workflow from the pushed tag, and attached to the
# GitHub release with its sha256. The coordinator then fetches those exact bytes
# (`python -m clusterbuck.release publish <url>` checks the attached digest), so the file
# on GitHub, the one joining nodes download and the one existing nodes update to are the
# same file — not three builds that happen to share a version number.
#
# --publish-via runs that command on the coordinator over ssh, as the service user. The
# coordinator must already run code that has `clusterbuck.release` (deploy it first).
# Paths default to the coordinator installer's; override with CBK_SERVER_PYTHON and
# CBK_RELEASE_JSON.
set -euo pipefail

die() { printf '\033[31m[release] %s\033[0m\n' "$*" >&2; exit 1; }
say() { printf '\033[36m[release]\033[0m %s\n' "$*"; }

VERSION="${1:-}"
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "usage: $0 X.Y.Z [--publish-via SSH_HOST]"
shift
PUBLISH_VIA=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --publish-via) PUBLISH_VIA="${2:?--publish-via needs an ssh host}"; shift 2 ;;
    *) die "unknown argument: $1" ;;
  esac
done
SERVER_PY="${CBK_SERVER_PYTHON:-/opt/clusterbuck/server/.venv/bin/python}"
RELEASE_JSON="${CBK_RELEASE_JSON:-/var/lib/clusterbuck/releases/release.json}"

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"
CONFIG=worker/src/cbk_worker/config.py

# ── preconditions ─────────────────────────────────────────────────────────────
[[ "$(git branch --show-current)" == main ]] || die "release from main"
[[ -z "$(git status --porcelain)" ]] || die "working tree is not clean"
git fetch -q origin main
[[ "$(git rev-parse HEAD)" == "$(git rev-parse origin/main)" ]] || die "main is not origin/main"
git rev-parse -q --verify "refs/tags/v$VERSION" >/dev/null && die "tag v$VERSION exists"

CURRENT="$(sed -n 's/^AGENT_VERSION = "\(.*\)"/\1/p' "$CONFIG")"
newer() { [[ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -1)" == "$1" && "$1" != "$2" ]]; }
# Both sides refuse a build that is not strictly newer, so tagging one would publish
# something the whole fleet ignores.
newer "$VERSION" "$CURRENT" || die "$VERSION is not newer than $CURRENT"

# ── bump, prove, commit, tag ──────────────────────────────────────────────────
say "bumping worker $CURRENT → $VERSION"
sed -i.bak "s/^AGENT_VERSION = \"$CURRENT\"/AGENT_VERSION = \"$VERSION\"/" "$CONFIG"
sed -i.bak "s/^version = \"$CURRENT\"/version = \"$VERSION\"/" worker/pyproject.toml
rm -f "$CONFIG.bak" worker/pyproject.toml.bak
(cd worker && uv lock -q)

say "testing"
(cd worker && uv run pytest -q && uv run ruff check src tests build.py)
(cd worker && uv run python build.py >/dev/null)
BAKED="$(unzip -p worker/dist/cbk.pyz cbk_worker/config.py | sed -n 's/^AGENT_VERSION = "\(.*\)"/\1/p')"
[[ "$BAKED" == "$VERSION" ]] || die "local build reports $BAKED, not $VERSION"

git add "$CONFIG" worker/pyproject.toml worker/uv.lock
git commit -q -m "release: worker $VERSION"
git tag -a "v$VERSION" -m "worker $VERSION"
git push -q origin main "v$VERSION"
say "pushed main and v$VERSION"

# ── wait for CI to build and attach it ────────────────────────────────────────
REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
say "waiting for the release workflow"
RUN=""
for _ in $(seq 1 30); do
  RUN="$(gh run list -R "$REPO" --workflow release.yml --branch "v$VERSION" \
           --json databaseId -q '.[0].databaseId' 2>/dev/null || true)"
  [[ -n "$RUN" ]] && break
  sleep 5
done
[[ -n "$RUN" ]] || die "no release workflow run appeared for v$VERSION"
gh run watch -R "$REPO" "$RUN" --exit-status >/dev/null || die "release workflow failed: gh run view $RUN -R $REPO"

URL="https://github.com/$REPO/releases/download/v$VERSION/cbk.pyz"
SHA="$(curl -fsSL "$URL.sha256" | awk '{print $1}')"
[[ -n "$SHA" ]] || die "no sha256 published at $URL.sha256"
say "released: $URL"
say "sha256:   $SHA"

# ── publish to a coordinator ──────────────────────────────────────────────────
PUBLISH=("sudo" "-u" "clusterbuck" "$SERVER_PY" "-m" "clusterbuck.release" "publish"
         "$URL" "--sha256" "$SHA" "--release" "$RELEASE_JSON")
if [[ -n "$PUBLISH_VIA" ]]; then
  say "publishing on $PUBLISH_VIA"
  # shellcheck disable=SC2029  # expanded here on purpose: these are this script's values
  ssh "$PUBLISH_VIA" "${PUBLISH[*]}"
else
  say "to publish, run on the coordinator:"
  printf '    %s\n' "${PUBLISH[*]}"
fi
