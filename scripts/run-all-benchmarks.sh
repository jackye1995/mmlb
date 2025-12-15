#!/bin/bash
# Run All MMLB Benchmarks Script
# This script runs benchmarks in two phases:
# 1. Initial benchmark: Original configs (10-5, 20-5, 40-5) with 500k rows
# 2. Extended benchmark: Full matrix of all configs and row counts
#
# Usage:
#   ./scripts/run-all-benchmarks.sh YOUR_S3_BUCKET
#
# Example:
#   ./scripts/run-all-benchmarks.sh jack-lancedb-devland-us-east-1

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <S3_BUCKET>"
    echo ""
    echo "Example:"
    echo "  $0 jack-lancedb-devland-us-east-1"
    exit 1
fi

S3_BUCKET="$1"
OUTPUT_DIR="${HOME}/benchmark-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Ensure uv is in PATH
export PATH="$HOME/.local/bin:$PATH"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Change to mmlb directory
cd ~/mmlb

echo "=============================================="
echo "MMLB Benchmark Suite - $TIMESTAMP"
echo "=============================================="
echo "S3 Bucket: $S3_BUCKET"
echo "Output Directory: $OUTPUT_DIR"
echo ""

# Show configuration options
echo "Available configurations:"
uv run mmlb configs
echo ""

# ============================================
# Phase 1: Initial Benchmark
# ============================================
echo "=============================================="
echo "Phase 1: Initial Benchmark"
echo "=============================================="
echo "Configs: 10-5, 20-5, 40-5"
echo "Rows: 500k"
echo "Output: $OUTPUT_DIR/benchmark-initial-$TIMESTAMP.json"
echo ""

uv run mmlb run \
    -f all \
    -t all \
    -c 10-5 -c 20-5 -c 40-5 \
    -r 500k \
    -b "$S3_BUCKET" \
    -p "mmlb-initial-$TIMESTAMP" \
    -o "$OUTPUT_DIR/benchmark-initial-$TIMESTAMP.json" \
    2>&1 | tee "$OUTPUT_DIR/benchmark-initial-$TIMESTAMP.log"

echo ""
echo "Initial benchmark completed!"
echo ""

# ============================================
# Phase 2: Extended Benchmark
# ============================================
echo "=============================================="
echo "Phase 2: Extended Benchmark (Full Matrix)"
echo "=============================================="
echo "Configs: All 12 (concurrency 10/20/40/80 x upload size 5/10/20 MB)"
echo "Rows: 100k, 500k, 1M"
echo "Output: $OUTPUT_DIR/benchmark-extended-$TIMESTAMP.json"
echo ""
echo "WARNING: This will take several hours to complete!"
echo ""

uv run mmlb run \
    -f all \
    -t all \
    -c all \
    -r all \
    -b "$S3_BUCKET" \
    -p "mmlb-extended-$TIMESTAMP" \
    -o "$OUTPUT_DIR/benchmark-extended-$TIMESTAMP.json" \
    2>&1 | tee "$OUTPUT_DIR/benchmark-extended-$TIMESTAMP.log"

echo ""
echo "=============================================="
echo "All Benchmarks Completed!"
echo "=============================================="
echo ""
echo "Results saved to:"
echo "  Initial: $OUTPUT_DIR/benchmark-initial-$TIMESTAMP.json"
echo "  Extended: $OUTPUT_DIR/benchmark-extended-$TIMESTAMP.json"
echo ""
echo "Logs saved to:"
echo "  Initial: $OUTPUT_DIR/benchmark-initial-$TIMESTAMP.log"
echo "  Extended: $OUTPUT_DIR/benchmark-extended-$TIMESTAMP.log"
