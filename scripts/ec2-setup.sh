#!/bin/bash
# EC2 Setup Script for MMLB Benchmark
# This script sets up an Amazon Linux 2023 EC2 instance for running MMLB benchmarks
#
# IMPORTANT: Must use Amazon Linux 2023 (AL2023), NOT Amazon Linux 2!
# Lance requires GLIBC 2.27+, and AL2 only has GLIBC 2.26.
# AL2023 has GLIBC 2.34 which is compatible.
#
# Prerequisites:
# - Amazon Linux 2023 AMI (NOT Amazon Linux 2!)
# - IAM role with S3 access (admin-ec2 or similar)
# - Instance type: r5.8xlarge or similar (32 vCPUs, 256GB RAM recommended)
# - 100GB EBS volume
#
# Usage:
#   1. SSH into the EC2 instance
#   2. Run: curl -fsSL https://raw.githubusercontent.com/jackye1995/mmlb/main/scripts/ec2-setup.sh | bash
#   Or clone repo first then run: bash scripts/ec2-setup.sh

set -e

echo "=============================================="
echo "MMLB EC2 Setup Script"
echo "=============================================="

# Check GLIBC version
GLIBC_VERSION=$(ldd --version | head -1 | awk '{print $NF}')
echo "GLIBC version: $GLIBC_VERSION"
if [[ "$(printf '%s\n' "2.27" "$GLIBC_VERSION" | sort -V | head -1)" != "2.27" ]]; then
    echo "ERROR: Lance requires GLIBC 2.27+. Current version: $GLIBC_VERSION"
    echo "Please use Amazon Linux 2023 instead of Amazon Linux 2."
    exit 1
fi

# Step 1: System update and git installation
echo ""
echo "[1/6] Updating system and installing git..."
sudo dnf update -y
sudo dnf install -y git

# Step 2: Install Amazon Corretto 17 (Java 17)
echo ""
echo "[2/6] Installing Amazon Corretto 17 (Java 17)..."
sudo dnf install -y java-17-amazon-corretto-devel
java -version

# Step 3: Install uv (Python package manager)
echo ""
echo "[3/6] Installing uv (Python package manager)..."
curl -LsSf https://astral.sh/uv/install.sh | sh

# Add uv to PATH for this session
export PATH="$HOME/.local/bin:$PATH"

# Also add to .bashrc for future sessions
if ! grep -q '.local/bin' ~/.bashrc; then
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
fi

# Step 4: Clone the repository
echo ""
echo "[4/6] Cloning MMLB repository..."
cd ~
if [ -d "mmlb" ]; then
    echo "Repository already exists, pulling latest..."
    cd mmlb && git pull
else
    git clone https://github.com/jackye1995/mmlb.git
    cd mmlb
fi

# Step 5: Build Java modules
echo ""
echo "[5/6] Building Java modules (this may take a few minutes)..."
make install-java

# Step 6: Install Python dependencies
echo ""
echo "[6/6] Installing Python dependencies..."
uv sync

# Verify installation
echo ""
echo "=============================================="
echo "Setup Complete! Verifying installation..."
echo "=============================================="

echo ""
echo "Java version:"
java -version

echo ""
echo "uv version:"
uv --version

echo ""
echo "MMLB CLI version:"
uv run mmlb --version

echo ""
echo "Available configurations:"
uv run mmlb configs

echo ""
echo "=============================================="
echo "JAR Files Built:"
echo "=============================================="
ls -lh ~/mmlb/java/mmlb-spark-*/target/mmlb-*.jar 2>/dev/null | grep -v original

echo ""
echo "=============================================="
echo "Ready to run benchmarks!"
echo "=============================================="
echo ""
echo "Example commands:"
echo ""
echo "  # Run all benchmarks with all configs and 500k rows (original test)"
echo "  uv run mmlb run -f all -t all -c all -r 500k -b YOUR_S3_BUCKET -o results.json"
echo ""
echo "  # Run full matrix (all configs x all row counts)"
echo "  uv run mmlb run -f all -t all -c all -r all -b YOUR_S3_BUCKET -o full-results.json"
echo ""
echo "  # Run in background with nohup"
echo "  nohup uv run mmlb run -f all -t all -c all -r all -b YOUR_S3_BUCKET -o results.json > benchmark.log 2>&1 &"
echo ""
echo "  # Monitor progress"
echo "  tail -f benchmark.log"
echo ""
