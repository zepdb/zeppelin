#!/bin/bash
# Cloud-init script: installs Docker on Amazon Linux 2023
set -euo pipefail

# Install Docker
dnf install -y docker
systemctl enable docker
systemctl start docker

# Allow ec2-user to use Docker without sudo
usermod -aG docker ec2-user

# Signal completion
touch /tmp/userdata-complete
