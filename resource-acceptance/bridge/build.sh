#!/bin/sh
# Offline, dependency-read-only build. No source-module or global cache writes.
set -eu
umask 077
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REF=/var/tmp/ai-gateway-transparent-pipes-20261103/bridge-implementation/repo
SCRATCH=$(mktemp -d "$ROOT/.build-XXXXXXXX")
trap 'rm -rf -- "$SCRATCH"' EXIT HUP INT TERM
mkdir "$SCRATCH/cache" "$SCRATCH/tmp"
export GOCACHE="$SCRATCH/cache" GOTMPDIR="$SCRATCH/tmp" GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local CGO_ENABLED=0 GOWORK=off
BEFORE=$(sha256sum "$REF/go.mod" "$REF/go.sum")
gofmt -w "$ROOT/main.go"
go -C "$REF" build -mod=readonly -trimpath -ldflags='-s -w -buildid=' -o "$ROOT/probe" "$ROOT/main.go"
go -C "$REF" vet -mod=readonly "$ROOT/main.go"
test "$BEFORE" = "$(sha256sum "$REF/go.mod" "$REF/go.sum")"
chmod 0755 "$ROOT/probe"
"$ROOT/probe" selftest
PYTHONPYCACHEPREFIX="$SCRATCH/pycache" python3 -m py_compile "$ROOT/accept.py" "$ROOT/test_runner.py"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/test_runner.py"
sha256sum "$ROOT/main.go" "$ROOT/accept.py" "$ROOT/probe" "$ROOT/build.sh" "$ROOT/test_runner.py"
