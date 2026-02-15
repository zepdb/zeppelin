#!/bin/bash
# apply-security.sh — Apply security hardening to the live Zeppelin EC2 instance
#
# What this does:
#   1. Recreates Prometheus container WITHOUT public port mapping
#   2. Recreates Grafana container WITH authentication enabled
#   3. Revokes the Prometheus 9090 security group rule
#   4. Restricts SSH and Grafana SG rules to your IP
#
# Usage:
#   ./deploy/apply-security.sh          # Run from project root
#
# Prerequisites:
#   - deploy/.env must exist with EC2_KEY_FILE, AWS_REGION, etc.
#   - Instance must be running (check with ./deploy/ec2-bench.sh status)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="$SCRIPT_DIR/.state"
SG_NAME="zeppelin-bench-sg"

# Load env
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
else
    echo "ERROR: No deploy/.env found."
    exit 1
fi

: "${EC2_KEY_FILE:?EC2_KEY_FILE is required in deploy/.env}"
: "${AWS_REGION:=us-west-2}"

EC2_KEY_FILE="${EC2_KEY_FILE/#\~/$HOME}"

# Get instance IP
if [ ! -f "$STATE_DIR/instance-ip" ]; then
    echo "ERROR: No instance IP found. Is the instance running?"
    exit 1
fi
IP=$(cat "$STATE_DIR/instance-ip")
echo "Target instance: $IP"

# Get deployer IP
MY_IP=$(curl -sf --max-time 5 https://ifconfig.me 2>/dev/null || \
        curl -sf --max-time 5 https://api.ipify.org 2>/dev/null || \
        echo "")
if [ -z "$MY_IP" ]; then
    echo "ERROR: Could not determine your public IP."
    exit 1
fi
echo "Your IP: $MY_IP"

# Generate Grafana admin password
GRAFANA_PASS=$(openssl rand -base64 24 | tr -d '/+=' | head -c 20)
echo ""
echo "================================================"
echo "  Grafana admin password: $GRAFANA_PASS"
echo "  SAVE THIS — it will not be shown again."
echo "================================================"
echo ""

SSH_CMD="ssh -o StrictHostKeyChecking=no -i $EC2_KEY_FILE ec2-user@$IP"

# --- 1. Recreate Prometheus without public port ---
echo "[1/4] Recreating Prometheus without public port mapping..."
$SSH_CMD bash -s << 'REMOTE_PROM'
docker stop prometheus 2>/dev/null || true
docker rm prometheus 2>/dev/null || true

# Ensure monitoring network exists
docker network create zeppelin-mon 2>/dev/null || true

docker run -d --name prometheus \
    --network zeppelin-mon \
    --add-host host.docker.internal:host-gateway \
    -v /home/ec2-user/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro \
    --restart unless-stopped \
    prom/prometheus:latest

echo "Prometheus recreated (no public port)."
REMOTE_PROM

# --- 2. Recreate Grafana with auth ---
echo "[2/4] Recreating Grafana with authentication..."
$SSH_CMD bash -s << REMOTE_GRAF
docker stop grafana 2>/dev/null || true
docker rm grafana 2>/dev/null || true

docker run -d --name grafana \
    --network zeppelin-mon \
    -p 3000:3000 \
    -e GF_AUTH_ANONYMOUS_ENABLED=false \
    -e GF_AUTH_DISABLE_LOGIN_FORM=false \
    -e GF_SECURITY_ADMIN_USER=admin \
    -e GF_SECURITY_ADMIN_PASSWORD=$GRAFANA_PASS \
    -v /home/ec2-user/zeppelin-src/demo/monitoring/grafana/provisioning:/etc/grafana/provisioning:ro \
    --restart unless-stopped \
    grafana/grafana:latest

echo "Grafana recreated with auth (user: admin)."
REMOTE_GRAF

# --- 3. Revoke Prometheus SG rule ---
echo "[3/4] Revoking Prometheus security group rule..."
SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=$SG_NAME" \
    --query 'SecurityGroups[0].GroupId' \
    --output text \
    --region "$AWS_REGION" 2>/dev/null)

if [ "$SG_ID" != "None" ] && [ -n "$SG_ID" ]; then
    # Revoke Prometheus port 9090 (ignore error if rule doesn't exist)
    aws ec2 revoke-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp --port 9090 --cidr 0.0.0.0/0 \
        --region "$AWS_REGION" 2>/dev/null && \
        echo "  Revoked: port 9090 from 0.0.0.0/0" || \
        echo "  Port 9090 rule already removed or not found."

    # --- 4. Restrict SSH to deployer IP ---
    echo "[4/4] Restricting SSH and adding Grafana SG rule..."

    # Revoke old SSH 0.0.0.0/0 rule
    aws ec2 revoke-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp --port 22 --cidr 0.0.0.0/0 \
        --region "$AWS_REGION" 2>/dev/null && \
        echo "  Revoked: SSH from 0.0.0.0/0" || \
        echo "  SSH 0.0.0.0/0 rule already removed."

    # Add SSH restricted to deployer IP
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp --port 22 --cidr "$MY_IP/32" \
        --region "$AWS_REGION" 2>/dev/null && \
        echo "  Added: SSH from $MY_IP/32" || \
        echo "  SSH rule for $MY_IP/32 already exists."

    # Add Grafana restricted to deployer IP
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp --port 3000 --cidr "$MY_IP/32" \
        --region "$AWS_REGION" 2>/dev/null && \
        echo "  Added: Grafana (3000) from $MY_IP/32" || \
        echo "  Grafana rule for $MY_IP/32 already exists."

    # Revoke Grafana 0.0.0.0/0 if it exists
    aws ec2 revoke-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp --port 3000 --cidr 0.0.0.0/0 \
        --region "$AWS_REGION" 2>/dev/null && \
        echo "  Revoked: Grafana from 0.0.0.0/0" || \
        echo "  Grafana 0.0.0.0/0 rule already removed."
else
    echo "  WARNING: Security group '$SG_NAME' not found. Skipping SG changes."
fi

# --- Verification ---
echo ""
echo "=== Verification ==="

echo -n "Zeppelin API (8080): "
if curl -sf --max-time 5 "http://$IP:8080/healthz" > /dev/null 2>&1; then
    echo "REACHABLE (healthy)"
else
    echo "NOT REACHABLE (check if Zeppelin is running)"
fi

echo -n "Prometheus (9090): "
if curl -sf --max-time 3 "http://$IP:9090" > /dev/null 2>&1; then
    echo "STILL REACHABLE — security group may need manual review"
else
    echo "BLOCKED (good)"
fi

echo -n "Grafana (3000): "
HTTP_CODE=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" "http://$IP:3000/login" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    echo "REACHABLE (login page — auth enabled)"
elif [ "$HTTP_CODE" = "000" ]; then
    echo "BLOCKED by SG (expected if not from $MY_IP)"
else
    echo "HTTP $HTTP_CODE"
fi

echo ""
echo "=== Security hardening applied ==="
echo "Grafana: http://$IP:3000 (user: admin, password: saved above)"
echo "API:     http://$IP:8080/healthz"
echo ""
echo "To verify from another network:"
echo "  nmap -p 22,3000,9090 $IP   # Should show filtered/closed"
echo "  curl http://$IP:8080/healthz # Should still work"
