/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.mmlb.benchmark.lance;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.mmlb.benchmark.BenchmarkConfig;
import org.mmlb.benchmark.SparkBenchmarkBase;
import static org.apache.spark.sql.functions.col;

public class LanceBenchmark extends SparkBenchmarkBase {
    public LanceBenchmark(BenchmarkConfig config) { super(config); }

    @Override protected String getFormatName() { return "Lance"; }
    @Override protected String getTableName(String baseName) { return "lance.default." + baseName + tableSuffix; }
    @Override protected void cleanupTable(String tableName) { try { spark.sql("DROP TABLE IF EXISTS " + tableName); } catch (Exception e) { } }

    @Override protected long writeData(Dataset<Row> data, String tableName) {
        cleanupTable(tableName);
        long start = System.currentTimeMillis();
        data.coalesce(1).writeTo(tableName).create();
        return System.currentTimeMillis() - start;
    }

    @Override protected long readData(String tableName) {
        long start = System.currentTimeMillis();
        // collect() forces full materialization - all rows are read from S3 and pulled to driver
        Row[] rows = (Row[]) spark.table(tableName).select(col("embedding")).collect();
        long elapsed = System.currentTimeMillis() - start;
        // Verify we actually read the data
        if (rows.length != config.getNumRows()) {
            System.err.printf("WARNING: Expected %d rows but got %d%n", config.getNumRows(), rows.length);
        }
        return elapsed;
    }

    @Override protected void initializeSpark() {
        spark = createBaseBuilder()
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance.impl", "dir")
            .config("spark.sql.catalog.lance.batch_size", String.valueOf(config.getBatchSize()))
            .config("spark.sql.catalog.lance.max_row_per_file", String.valueOf(config.getMaxRowsPerFile()))
            .config("spark.sql.catalog.lance.root", "s3://" + config.getS3Bucket() + "/" + config.getS3Prefix())
            .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }
}
