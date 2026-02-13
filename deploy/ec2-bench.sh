#!/bin/bash
# ec2-bench.sh — One-command EC2 provisioning for Zeppelin benchmarks
#
# Usage:
#   ./deploy/ec2-bench.sh launch     # Provision EC2, deploy Zeppelin
#   ./deploy/ec2-bench.sh status     # Show instance status
#   ./deploy/ec2-bench.sh ssh        # SSH into the instance
#   ./deploy/ec2-bench.sh deploy     # Re-deploy (rebuild + transfer image)
#   ./deploy/ec2-bench.sh teardown   # Terminate instance + cleanup
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
STATE_DIR="$SCRIPT_DIR/.state"
SG_NAME="zeppelin-bench-sg"

mkdir -p "$STATE_DIR"

# Load env
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
elif [ -f "$SCRIPT_DIR/.env.example" ]; then
    echo "ERROR: No deploy/.env found. Copy deploy/.env.example to deploy/.env and fill in values."
    exit 1
fi

: "${EC2_KEY_NAME:?EC2_KEY_NAME is required in deploy/.env}"
: "${EC2_KEY_FILE:?EC2_KEY_FILE is required in deploy/.env}"
: "${S3_BUCKET:?S3_BUCKET is required in deploy/.env}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required in deploy/.env}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required in deploy/.env}"
: "${AWS_REGION:=us-west-2}"
: "${EC2_INSTANCE_TYPE:=c7i.8xlarge}"

EC2_KEY_FILE="${EC2_KEY_FILE/#\~/$HOME}"

# Resolve latest Amazon Linux 2023 AMI
resolve_ami() {
    aws ec2 describe-images \
        --owners amazon \
        --filters "Name=name,Values=al2023-ami-2023*-x86_64" \
                  "Name=state,Values=available" \
        --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' \
        --output text \
        --region "$AWS_REGION"
}

get_instance_id() {
    if [ -f "$STATE_DIR/instance-id" ]; then
        cat "$STATE_DIR/instance-id"
    else
        echo ""
    fi
}

get_instance_ip() {
    local instance_id="$1"
    aws ec2 describe-instances \
        --instance-ids "$instance_id" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null
}

get_instance_state() {
    local instance_id="$1"
    aws ec2 describe-instances \
        --instance-ids "$instance_id" \
        --query 'Reservations[0].Instances[0].State.Name' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null
}

wait_for_ssh() {
    local ip="$1"
    echo "Waiting for SSH to become available..."
    for i in $(seq 1 60); do
        if ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
            -i "$EC2_KEY_FILE" "ec2-user@$ip" "test -f /tmp/userdata-complete" 2>/dev/null; then
            echo "Instance is ready."
            return 0
        fi
        printf "."
        sleep 5
    done
    echo ""
    echo "WARNING: Timed out waiting for instance setup (5 min). SSH may still work."
    return 1
}

create_security_group() {
    local sg_id
    sg_id=$(aws ec2 describe-security-groups \
        --filters "Name=group-name,Values=$SG_NAME" \
        --query 'SecurityGroups[0].GroupId' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null)

    if [ "$sg_id" != "None" ] && [ -n "$sg_id" ]; then
        echo "$sg_id"
        return 0
    fi

    echo "Creating security group: $SG_NAME" >&2
    sg_id=$(aws ec2 create-security-group \
        --group-name "$SG_NAME" \
        --description "Zeppelin benchmark instance" \
        --output text \
        --query 'GroupId' \
        --region "$AWS_REGION")

    # SSH
    aws ec2 authorize-security-group-ingress \
        --group-id "$sg_id" \
        --protocol tcp --port 22 --cidr 0.0.0.0/0 \
        --region "$AWS_REGION" > /dev/null

    # Zeppelin API
    aws ec2 authorize-security-group-ingress \
        --group-id "$sg_id" \
        --protocol tcp --port 8080 --cidr 0.0.0.0/0 \
        --region "$AWS_REGION" > /dev/null

    # Metrics
    aws ec2 authorize-security-group-ingress \
        --group-id "$sg_id" \
        --protocol tcp --port 9090 --cidr 0.0.0.0/0 \
        --region "$AWS_REGION" > /dev/null

    echo "$sg_id"
}

build_and_transfer_image() {
    local ip="$1"

    echo "Building Docker image (linux/amd64) with profiling support..."
    docker build --platform linux/amd64 --build-arg FEATURES=profiling -t zeppelin:latest "$PROJECT_ROOT"

    echo "Saving Docker image..."
    docker save zeppelin:latest | gzip > "$STATE_DIR/zeppelin-image.tar.gz"

    echo "Transferring image to EC2 (~1-2 min)..."
    scp -o StrictHostKeyChecking=no -i "$EC2_KEY_FILE" \
        "$STATE_DIR/zeppelin-image.tar.gz" \
        "ec2-user@$ip:/tmp/zeppelin-image.tar.gz"

    echo "Loading image on EC2..."
    ssh -o StrictHostKeyChecking=no -i "$EC2_KEY_FILE" "ec2-user@$ip" \
        "gunzip -c /tmp/zeppelin-image.tar.gz | docker load && rm /tmp/zeppelin-image.tar.gz"

    # Transfer run script and env
    scp -o StrictHostKeyChecking=no -i "$EC2_KEY_FILE" \
        "$SCRIPT_DIR/run-server.sh" \
        "ec2-user@$ip:/home/ec2-user/run-server.sh"

    ssh -o StrictHostKeyChecking=no -i "$EC2_KEY_FILE" "ec2-user@$ip" \
        "chmod +x /home/ec2-user/run-server.sh"

    # Create .env on instance with credentials
    ssh -o StrictHostKeyChecking=no -i "$EC2_KEY_FILE" "ec2-user@$ip" \
        "cat > /home/ec2-user/.env << 'ENVEOF'
S3_BUCKET=$S3_BUCKET
AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
AWS_REGION=$AWS_REGION
ENVEOF"
}

cmd_launch() {
    local existing_id
    existing_id=$(get_instance_id)
    if [ -n "$existing_id" ]; then
        local state
        state=$(get_instance_state "$existing_id")
        if [ "$state" == "running" ]; then
            echo "Instance $existing_id is already running."
            echo "Use './deploy/ec2-bench.sh teardown' first, or './deploy/ec2-bench.sh deploy' to re-deploy."
            exit 1
        fi
    fi

    echo "=== Launching Zeppelin Benchmark Instance ==="
    echo "Instance type: $EC2_INSTANCE_TYPE"
    echo "Region: $AWS_REGION"
    echo "S3 bucket: $S3_BUCKET"
    echo ""

    # Resolve AMI
    echo "Resolving latest Amazon Linux 2023 AMI..."
    local ami_id
    ami_id=$(resolve_ami)
    echo "AMI: $ami_id"

    # Create security group
    local sg_id
    sg_id=$(create_security_group)
    echo "Security group: $sg_id"

    # Launch instance
    echo "Launching EC2 instance..."
    local instance_id
    instance_id=$(aws ec2 run-instances \
        --image-id "$ami_id" \
        --instance-type "$EC2_INSTANCE_TYPE" \
        --key-name "$EC2_KEY_NAME" \
        --security-group-ids "$sg_id" \
        --user-data "file://$SCRIPT_DIR/ec2-userdata.sh" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=zeppelin-bench}]" \
        --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3}" \
        --query 'Instances[0].InstanceId' \
        --output text \
        --region "$AWS_REGION")

    echo "$instance_id" > "$STATE_DIR/instance-id"
    echo "Instance: $instance_id"

    # Wait for running
    echo "Waiting for instance to start..."
    aws ec2 wait instance-running \
        --instance-ids "$instance_id" \
        --region "$AWS_REGION"

    local ip
    ip=$(get_instance_ip "$instance_id")
    echo "$ip" > "$STATE_DIR/instance-ip"
    echo "Public IP: $ip"

    # Wait for userdata to complete (Docker install)
    wait_for_ssh "$ip"

    # Build and transfer
    build_and_transfer_image "$ip"

    # Start server
    echo "Starting Zeppelin..."
    ssh -o StrictHostKeyChecking=no -i "$EC2_KEY_FILE" "ec2-user@$ip" \
        "/home/ec2-user/run-server.sh"

    echo ""
    echo "=== Zeppelin is running ==="
    echo "API:     http://$ip:8080"
    echo "Health:  http://$ip:8080/healthz"
    echo "SSH:     ./deploy/ec2-bench.sh ssh"
    echo ""
    echo "Run benchmarks:"
    echo "  cargo run -p zeppelin-bench -- --target http://$ip:8080 --scenario ingest"
}

cmd_status() {
    local instance_id
    instance_id=$(get_instance_id)
    if [ -z "$instance_id" ]; then
        echo "No instance found. Run './deploy/ec2-bench.sh launch' first."
        exit 1
    fi

    local state ip
    state=$(get_instance_state "$instance_id")
    ip=$(get_instance_ip "$instance_id")

    echo "Instance: $instance_id"
    echo "State:    $state"
    echo "IP:       $ip"

    if [ "$state" == "running" ]; then
        echo ""
        echo "API:     http://$ip:8080"
        echo "Health:  http://$ip:8080/healthz"

        if curl -sf "http://$ip:8080/healthz" > /dev/null 2>&1; then
            echo "Status:  HEALTHY"
        else
            echo "Status:  NOT RESPONDING (server may still be starting)"
        fi
    fi
}

cmd_ssh() {
    local instance_id ip
    instance_id=$(get_instance_id)
    if [ -z "$instance_id" ]; then
        echo "No instance found."
        exit 1
    fi

    ip=$(get_instance_ip "$instance_id")
    echo "Connecting to $ip..."
    ssh -o StrictHostKeyChecking=no -i "$EC2_KEY_FILE" "ec2-user@$ip"
}

cmd_deploy() {
    local instance_id ip
    instance_id=$(get_instance_id)
    if [ -z "$instance_id" ]; then
        echo "No instance found. Run './deploy/ec2-bench.sh launch' first."
        exit 1
    fi

    ip=$(get_instance_ip "$instance_id")
    echo "Re-deploying to $ip..."

    build_and_transfer_image "$ip"

    echo "Starting Zeppelin..."
    ssh -o StrictHostKeyChecking=no -i "$EC2_KEY_FILE" "ec2-user@$ip" \
        "/home/ec2-user/run-server.sh"

    echo "Done. API: http://$ip:8080"
}

cmd_teardown() {
    local instance_id
    instance_id=$(get_instance_id)
    if [ -z "$instance_id" ]; then
        echo "No instance found."
        exit 0
    fi

    echo "Terminating instance $instance_id..."
    aws ec2 terminate-instances \
        --instance-ids "$instance_id" \
        --region "$AWS_REGION" > /dev/null

    echo "Waiting for termination..."
    aws ec2 wait instance-terminated \
        --instance-ids "$instance_id" \
        --region "$AWS_REGION" 2>/dev/null || true

    # Try to delete security group (may fail if still in use briefly)
    local sg_id
    sg_id=$(aws ec2 describe-security-groups \
        --filters "Name=group-name,Values=$SG_NAME" \
        --query 'SecurityGroups[0].GroupId' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null)

    if [ "$sg_id" != "None" ] && [ -n "$sg_id" ]; then
        echo "Deleting security group $sg_id..."
        sleep 5  # Brief wait for ENI detach
        aws ec2 delete-security-group --group-id "$sg_id" --region "$AWS_REGION" 2>/dev/null || \
            echo "Could not delete security group yet — it will be cleaned up on next launch."
    fi

    # Clean state
    rm -rf "$STATE_DIR"
    mkdir -p "$STATE_DIR"

    echo "Teardown complete."
}

# Main
case "${1:-help}" in
    launch)   cmd_launch ;;
    status)   cmd_status ;;
    ssh)      cmd_ssh ;;
    deploy)   cmd_deploy ;;
    teardown) cmd_teardown ;;
    *)
        echo "Usage: $0 {launch|status|ssh|deploy|teardown}"
        echo ""
        echo "Commands:"
        echo "  launch    Provision EC2 instance and deploy Zeppelin"
        echo "  status    Show instance status and health"
        echo "  ssh       SSH into the instance"
        echo "  deploy    Re-build and re-deploy (without re-provisioning)"
        echo "  teardown  Terminate instance and clean up"
        exit 1
        ;;
esac
