#!/usr/bin/env python3
"""MMLB - Multi-Modal Lakehouse Benchmark CLI"""

import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

console = Console()

# Get the mmlb package directory (parent of src/mmlb)
MMLB_ROOT = Path(__file__).parent.parent.parent
JAVA_DIR = MMLB_ROOT / "java"

# Default JAR paths relative to mmlb root
DEFAULT_LANCE_JAR = JAVA_DIR / "mmlb-spark-lance" / "target" / "mmlb-spark-lance-0.1.0.jar"
DEFAULT_ICEBERG_JAR = JAVA_DIR / "mmlb-spark-iceberg" / "target" / "mmlb-spark-iceberg-0.1.0.jar"

# JVM options for Java 17+
JVM_OPTS = [
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
]

# Lance configuration presets (format: concurrency:uploadSizeMB)
# 5MB = 5242880 bytes
LANCE_CONFIGS = {
    "default": {"LANCE_UPLOAD_CONCURRENCY": "10", "LANCE_INITIAL_UPLOAD_SIZE": "5242880"},      # 10:5
    "20-5": {"LANCE_UPLOAD_CONCURRENCY": "20", "LANCE_INITIAL_UPLOAD_SIZE": "5242880"},        # 20:5
    "40-5": {"LANCE_UPLOAD_CONCURRENCY": "40", "LANCE_INITIAL_UPLOAD_SIZE": "5242880"},        # 40:5
}


def check_jar_exists(jar_path: Path, format_name: str) -> bool:
    """Check if a JAR file exists."""
    if not jar_path.exists():
        console.print(f"[red]Error: {format_name} JAR not found at {jar_path}[/red]")
        console.print("[yellow]Build the JAR first: cd java && mvn clean package -DskipTests[/yellow]")
        return False
    return True


def parse_benchmark_json(output: str) -> Optional[dict]:
    """Parse benchmark result JSON from output."""
    # Look for JSON block in output
    json_match = re.search(r'\{[^{}]*"formatName"[^{}]*\}', output, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    return None


def run_benchmark(
    jar_path: Path,
    benchmark_type: str,
    env: dict,
    format_name: str,
    config_name: str = "default",
) -> Optional[dict]:
    """Run a benchmark with the specified JAR and return results."""
    console.print(f"\n[cyan]Running {format_name} ({config_name}) {benchmark_type} benchmark...[/cyan]")

    cmd = ["java", "-Xmx12g", "-XX:+UseG1GC"] + JVM_OPTS + ["-jar", str(jar_path), benchmark_type]

    full_env = os.environ.copy()
    full_env.update(env)

    # Show config
    config_items = {k: v for k, v in env.items() if k.startswith(("BENCHMARK_", "LANCE_"))}
    if config_items:
        table = Table(title=f"{format_name} ({config_name}) Configuration", show_header=False)
        for k, v in config_items.items():
            table.add_row(k, v)
        console.print(table)

    process = subprocess.Popen(cmd, env=full_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    output_lines = []
    for line in process.stdout:
        print(line, end="")
        output_lines.append(line)

    return_code = process.wait()
    output = "".join(output_lines)

    # Parse result JSON
    result = parse_benchmark_json(output)
    if result:
        result["configName"] = config_name
        result["returnCode"] = return_code

    return result


def generate_comparison_report(results: list[dict], rows: int, embedding_dim: int) -> dict:
    """Generate a comparison report from benchmark results."""
    report = {
        "timestamp": datetime.now().isoformat(),
        "parameters": {
            "numRows": rows,
            "embeddingDim": embedding_dim,
        },
        "results": {},
        "comparison": {
            "write": {},
            "read": {},
        }
    }

    # Organize results by format+config
    for r in results:
        if not r:
            continue
        key = f"{r.get('formatName', 'Unknown')}_{r.get('configName', 'default')}"
        report["results"][key] = r

        # Extract metrics for comparison
        if r.get("avgWriteTimeMs"):
            report["comparison"]["write"][key] = {
                "avgTimeMs": r["avgWriteTimeMs"],
                "throughput": r.get("writeThroughput", 0),
            }
        if r.get("avgReadTimeMs"):
            report["comparison"]["read"][key] = {
                "avgTimeMs": r["avgReadTimeMs"],
                "throughput": r.get("readThroughput", 0),
            }

    # Calculate relative performance (vs Iceberg baseline)
    iceberg_write = report["comparison"]["write"].get("Iceberg_default", {})
    iceberg_read = report["comparison"]["read"].get("Iceberg_default", {})

    if iceberg_write.get("avgTimeMs"):
        for key, metrics in report["comparison"]["write"].items():
            if key != "Iceberg_default" and metrics.get("avgTimeMs"):
                metrics["vsIcebergRatio"] = round(iceberg_write["avgTimeMs"] / metrics["avgTimeMs"], 2)

    if iceberg_read.get("avgTimeMs"):
        for key, metrics in report["comparison"]["read"].items():
            if key != "Iceberg_default" and metrics.get("avgTimeMs"):
                metrics["vsIcebergRatio"] = round(iceberg_read["avgTimeMs"] / metrics["avgTimeMs"], 2)

    return report


def print_comparison_table(report: dict):
    """Print a formatted comparison table."""
    console.print("\n" + "=" * 80)
    console.print("[bold cyan]BENCHMARK COMPARISON SUMMARY[/bold cyan]")
    console.print("=" * 80)

    params = report["parameters"]
    console.print(f"Rows: {params['numRows']:,}, Embedding Dim: {params['embeddingDim']}")

    # Write comparison
    if report["comparison"]["write"]:
        table = Table(title="Write Performance")
        table.add_column("Format/Config", style="cyan")
        table.add_column("Avg Time (ms)", justify="right")
        table.add_column("Throughput (rows/s)", justify="right")
        table.add_column("vs Iceberg", justify="right")

        for key, metrics in sorted(report["comparison"]["write"].items()):
            ratio = metrics.get("vsIcebergRatio", "-")
            ratio_str = f"{ratio}x" if isinstance(ratio, (int, float)) else ratio
            table.add_row(
                key.replace("_", " "),
                f"{metrics['avgTimeMs']:.0f}",
                f"{metrics['throughput']:.0f}",
                ratio_str,
            )
        console.print(table)

    # Read comparison
    if report["comparison"]["read"]:
        table = Table(title="Read Performance")
        table.add_column("Format/Config", style="cyan")
        table.add_column("Avg Time (ms)", justify="right")
        table.add_column("Throughput (rows/s)", justify="right")
        table.add_column("vs Iceberg", justify="right")

        for key, metrics in sorted(report["comparison"]["read"].items()):
            ratio = metrics.get("vsIcebergRatio", "-")
            ratio_str = f"{ratio}x" if isinstance(ratio, (int, float)) else ratio
            table.add_row(
                key.replace("_", " "),
                f"{metrics['avgTimeMs']:.0f}",
                f"{metrics['throughput']:.0f}",
                ratio_str,
            )
        console.print(table)


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """MMLB - Multi-Modal Lakehouse Benchmark CLI

    Benchmark tool for comparing Lance vs Iceberg/Parquet performance on Spark.
    """
    pass


@cli.command()
@click.option("--format", "-f", type=click.Choice(["lance", "iceberg", "all"]), default="all", help="Format to benchmark")
@click.option("--type", "-t", "benchmark_type", type=click.Choice(["write", "read", "all"]), default="all", help="Benchmark type")
@click.option("--rows", "-n", type=int, default=100000, help="Number of rows to generate")
@click.option("--embedding-dim", "-d", type=int, default=768, help="Embedding dimension")
@click.option("--s3-bucket", "-b", required=True, envvar="BENCHMARK_S3_BUCKET", help="S3 bucket for data")
@click.option("--s3-prefix", "-p", default="mmlb-benchmark", envvar="BENCHMARK_S3_PREFIX", help="S3 prefix")
@click.option("--config", "-c", "configs", type=click.Choice(list(LANCE_CONFIGS.keys()) + ["all"]), multiple=True, default=["default"], help="Lance config preset(s). Use 'all' to run all configs, or specify multiple with -c default -c 20-5")
@click.option("--lance-jar", type=click.Path(exists=False), default=None, envvar="MMLB_LANCE_JAR", help="Path to Lance benchmark JAR")
@click.option("--iceberg-jar", type=click.Path(exists=False), default=None, envvar="MMLB_ICEBERG_JAR", help="Path to Iceberg benchmark JAR")
@click.option("--output", "-o", type=click.Path(), default=None, help="Output file for comparison JSON")
def run(format, benchmark_type, rows, embedding_dim, s3_bucket, s3_prefix, configs, lance_jar, iceberg_jar, output):
    """Run benchmarks for Lance and/or Iceberg.

    Examples:

      # Run all benchmarks with all Lance configs
      mmlb run -f all -t all -c all -b my-bucket -n 10000

      # Run specific configs
      mmlb run -f lance -t write -c default -c 20-5 -b my-bucket
    """

    # Resolve JAR paths
    lance_jar_path = Path(lance_jar) if lance_jar else DEFAULT_LANCE_JAR
    iceberg_jar_path = Path(iceberg_jar) if iceberg_jar else DEFAULT_ICEBERG_JAR

    # Resolve configs
    if "all" in configs:
        lance_configs_to_run = list(LANCE_CONFIGS.keys())
    else:
        lance_configs_to_run = list(configs)

    # Base environment
    base_env = {
        "BENCHMARK_NUM_ROWS": str(rows),
        "BENCHMARK_EMBEDDING_DIM": str(embedding_dim),
        "BENCHMARK_S3_BUCKET": s3_bucket,
        "BENCHMARK_S3_PREFIX": s3_prefix,
    }

    formats_to_run = ["lance", "iceberg"] if format == "all" else [format]
    benchmark_types = ["write", "read"] if benchmark_type == "all" else [benchmark_type]

    all_results = []

    # Run Iceberg first (baseline)
    if "iceberg" in formats_to_run:
        if check_jar_exists(iceberg_jar_path, "Iceberg"):
            for bt in benchmark_types:
                env = base_env.copy()
                env["BENCHMARK_CONFIG_NAME"] = "default"
                result = run_benchmark(iceberg_jar_path, bt, env, "Iceberg", "default")
                if result:
                    all_results.append(result)

    # Run Lance with each config
    if "lance" in formats_to_run:
        if check_jar_exists(lance_jar_path, "Lance"):
            for config_name in lance_configs_to_run:
                for bt in benchmark_types:
                    env = base_env.copy()
                    env["BENCHMARK_CONFIG_NAME"] = config_name
                    env.update(LANCE_CONFIGS.get(config_name, LANCE_CONFIGS["default"]))
                    result = run_benchmark(lance_jar_path, bt, env, "Lance", config_name)
                    if result:
                        all_results.append(result)

    # Generate and print comparison report
    if all_results:
        report = generate_comparison_report(all_results, rows, embedding_dim)
        print_comparison_table(report)

        # Output JSON
        console.print("\n[bold]Full Comparison JSON:[/bold]")
        json_output = json.dumps(report, indent=2)
        console.print(json_output)

        if output:
            with open(output, "w") as f:
                f.write(json_output)
            console.print(f"\n[green]Results saved to {output}[/green]")


@cli.command()
def configs():
    """List available Lance configuration presets."""
    table = Table(title="Lance Configuration Presets")
    table.add_column("Name", style="cyan")
    table.add_column("Upload Concurrency")
    table.add_column("Initial Upload Size")

    for name, cfg in LANCE_CONFIGS.items():
        size_mb = int(cfg["LANCE_INITIAL_UPLOAD_SIZE"]) / (1024 * 1024)
        table.add_row(name, cfg["LANCE_UPLOAD_CONCURRENCY"], f"{size_mb:.0f} MB")

    console.print(table)


def main():
    cli()


if __name__ == "__main__":
    main()
