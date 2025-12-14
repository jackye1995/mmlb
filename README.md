# MMLB - Multi-Modal Lakehouse Benchmark

## Installation

```bash
# Install Java modules
make install-java

# Install Python CLI
make install-python
```

## Usage

```bash
# Run benchmark with default config
mmlb run -f lance -t write -n 10000 -b your-s3-bucket

# Run with specific Lance config
mmlb run -f lance -t write -n 10000 -b your-s3-bucket -c 20-5

# List available configs
mmlb configs
```

## Available Lance Configs

- `default` - Concurrency 10, Upload size 5MB
- `20-5` - Concurrency 20, Upload size 5MB
- `40-5` - Concurrency 40, Upload size 5MB
