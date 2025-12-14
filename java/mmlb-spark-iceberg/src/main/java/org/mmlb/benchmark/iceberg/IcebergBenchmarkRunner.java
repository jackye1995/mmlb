/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.mmlb.benchmark.iceberg;

import org.mmlb.benchmark.BenchmarkConfig;
import org.mmlb.benchmark.BenchmarkResult;

public class IcebergBenchmarkRunner {
    public static void main(String[] args) {
        String type = args.length > 0 ? args[0].toLowerCase() : "all";
        BenchmarkConfig config = new BenchmarkConfig();
        System.out.println("Starting Iceberg Benchmark\nConfiguration: " + config);
        IcebergBenchmark benchmark = new IcebergBenchmark(config);
        try {
            benchmark.start();
            BenchmarkResult result = switch (type) {
                case "write" -> benchmark.runWriteBenchmark();
                case "read" -> benchmark.runReadBenchmark();
                default -> benchmark.runReadWriteBenchmark();
            };
            System.out.println("\n=== Benchmark Complete ===\n" + result.toJson());
        } finally { benchmark.stop(); }
    }
}
