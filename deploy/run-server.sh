#!/bin/bash
# Runs Zeppelin container on EC2 with S3 backend
set -euo pipefail

# Source env vars if .env exists
if [ -f /home/ec2-user/.env ]; then
    set -a
    source /home/ec2-user/.env
    set +a
fi

# Required env vars
: "${S3_BUCKET:?S3_BUCKET is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"
: "${AWS_REGION:=us-west-2}"

# Stop existing container if running
docker rm -f zeppelin 2>/dev/null || true

docker run -d --name zeppelin \
    --restart unless-stopped \
    -p 8080:8080 \
    -e STORAGE_BACKEND=s3 \
    -e S3_BUCKET="$S3_BUCKET" \
    -e AWS_REGION="$AWS_REGION" \
    -e AWS_DEFAULT_REGION="$AWS_REGION" \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -e RUST_LOG=info \
    -e ZEPPELIN_MAX_WAL_FRAGMENTS="${ZEPPELIN_MAX_WAL_FRAGMENTS:-5}" \
    -e ZEPPELIN_COMPACTION_INTERVAL_SECS="${ZEPPELIN_COMPACTION_INTERVAL_SECS:-10}" \
    -e ZEPPELIN_DEFAULT_NUM_CENTROIDS="${ZEPPELIN_DEFAULT_NUM_CENTROIDS:-256}" \
    -e ZEPPELIN_DEFAULT_NPROBE="${ZEPPELIN_DEFAULT_NPROBE:-4}" \
    -e ZEPPELIN_REQUEST_TIMEOUT_SECS="${ZEPPELIN_REQUEST_TIMEOUT_SECS:-600}" \
    -e ZEPPELIN_BITMAP_INDEX="${ZEPPELIN_BITMAP_INDEX:-true}" \
    -e ZEPPELIN_CACHE_MAX_SIZE_GB="${ZEPPELIN_CACHE_MAX_SIZE_GB:-20}" \
    -e ZEPPELIN_MEMORY_CACHE_MAX_MB="${ZEPPELIN_MEMORY_CACHE_MAX_MB:-2048}" \
    -e ZEPPELIN_FTS_INDEX="${ZEPPELIN_FTS_INDEX:-false}" \
    -e ZEPPELIN_HIERARCHICAL="${ZEPPELIN_HIERARCHICAL:-false}" \
    -e ZEPPELIN_BEAM_WIDTH="${ZEPPELIN_BEAM_WIDTH:-10}" \
    -e ZEPPELIN_LEAF_SIZE="${ZEPPELIN_LEAF_SIZE:-}" \
    zeppelin:latest

echo "Zeppelin started. Waiting for health check..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8080/healthz > /dev/null 2>&1; then
        echo "Zeppelin is healthy!"
        exit 0
    fi
    sleep 2
done

echo "WARNING: Health check did not pass within 60s"
docker logs zeppelin
exit 1
