#!/bin/bash
# Run Full MMLB Benchmark Matrix
#
# This script runs the complete benchmark matrix:
# - Formats: Lance, Iceberg
# - Benchmark types: write, read
# - Lance configs: 12 combinations (concurrency 10/20/40/80 x upload size 5/10/20 MB)
# - Row counts: 100k, 500k, 1M
#
# Total benchmarks: 156 (12 Lance configs x 3 row counts x 2 types + 1 Iceberg x 3 rows x 2 types)
#
# Usage:
#   ./scripts/run-full-benchmark.sh YOUR_S3_BUCKET [OUTPUT_DIR]

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <S3_BUCKET> [OUTPUT_DIR]"
    echo ""
    echo "Example:"
    echo "  $0 jack-lancedb-devland-us-east-1"
    echo "  $0 jack-lancedb-devland-us-east-1 ~/benchmark-results"
    exit 1
fi

S3_BUCKET="$1"
OUTPUT_DIR="${2:-$HOME/benchmark-results}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Ensure uv is in PATH
export PATH="$HOME/.local/bin:$PATH"

echo "=============================================="
echo "MMLB Full Benchmark Matrix"
echo "=============================================="
echo "S3 Bucket: $S3_BUCKET"
echo "Output Directory: $OUTPUT_DIR"
echo "Timestamp: $TIMESTAMP"
echo ""

# Change to mmlb directory
cd ~/mmlb

# Show what we're about to run
echo "Benchmark configuration:"
uv run mmlb configs
echo ""

# Run the full benchmark
echo "Starting full benchmark matrix..."
echo "This will take several hours to complete."
echo ""
echo "Output file: $OUTPUT_DIR/benchmark-full-$TIMESTAMP.json"
echo "Log file: $OUTPUT_DIR/benchmark-full-$TIMESTAMP.log"
echo ""

# Run with nohup so it continues even if SSH disconnects
nohup uv run mmlb run \
    -f all \
    -t all \
    -c all \
    -r all \
    -b "$S3_BUCKET" \
    -p "mmlb-benchmark-$TIMESTAMP" \
    -o "$OUTPUT_DIR/benchmark-full-$TIMESTAMP.json" \
    > "$OUTPUT_DIR/benchmark-full-$TIMESTAMP.log" 2>&1 &

PID=$!
echo "Benchmark started with PID: $PID"
echo ""
echo "To monitor progress:"
echo "  tail -f $OUTPUT_DIR/benchmark-full-$TIMESTAMP.log"
echo ""
echo "To check if still running:"
echo "  ps aux | grep mmlb"
echo ""
echo "Results will be saved to:"
echo "  $OUTPUT_DIR/benchmark-full-$TIMESTAMP.json"
