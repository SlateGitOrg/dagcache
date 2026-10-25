#!/usr/bin/env bash
# Run the S3 backend tests against a real S3-compatible server (SeaweedFS) in Docker.
# Skips cleanly (exit 0) when Docker is not available. Always removes its container.
#
# MinIO would be the obvious choice, but its official images are no longer pullable
# anonymously from Docker Hub/quay.io (checked 2026-10-25), so SeaweedFS's S3 gateway
# - an independent S3 implementation - is used instead. See DECISIONS.md.
set -euo pipefail

IMAGE="chrislusf/seaweedfs:3.97"
NAME="p50-42-dagcache-s3"
PORT="${DAGCACHE_S3_TEST_PORT:-19042}"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "integration: Docker not available - skipping"
  exit 0
fi

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

docker run -d --name "$NAME" -p "127.0.0.1:${PORT}:8333" "$IMAGE" \
  server -s3 -dir=/data -volume.max=64 -master.volumeSizeLimitMB=64 >/dev/null

ready=0
for _ in $(seq 1 120); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${PORT}/" || true)
  if [ "$code" != "000" ]; then ready=1; break; fi
  sleep 0.5
done
if [ "$ready" != 1 ]; then
  echo "integration: S3 server did not become ready"; docker logs "$NAME" | tail -20; exit 1
fi
sleep 2  # filer/volume registration settles after the S3 port opens

DAGCACHE_TEST_S3_ENDPOINT="http://127.0.0.1:${PORT}" \
AWS_ACCESS_KEY_ID=dagcache AWS_SECRET_ACCESS_KEY=dagcache-secret AWS_DEFAULT_REGION=us-east-1 \
  uv run --frozen pytest -q -m integration tests/integration
