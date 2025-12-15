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
    @Override protected void cleanupTable(String tableName) {
        // Lance catalog doesn't support DROP TABLE IF EXISTS, so we skip cleanup
        // Each benchmark run uses a unique table name with timestamp suffix anyway
    }

    private void createTable(String tableName) {
        // Create table with fixed-size-list for embedding column
        String sql = String.format(
            "CREATE TABLE %s (" +
            "  id BIGINT NOT NULL," +
            "  timestamp TIMESTAMP," +
            "  label STRING," +
            "  score DOUBLE," +
            "  embedding ARRAY<FLOAT> NOT NULL" +
            ") USING lance " +
            "TBLPROPERTIES ('embedding.arrow.fixed-size-list.size' = '%d')",
            tableName, config.getEmbeddingDim());
        spark.sql(sql);
    }

    @Override protected long writeData(Dataset<Row> data, String tableName) {
        cleanupTable(tableName);
        createTable(tableName);
        long start = System.currentTimeMillis();
        data.coalesce(1).write()
            .format("lance")
            .option("use_queued_write_buffer", "true")
            .mode("append")
            .saveAsTable(tableName);
        return System.currentTimeMillis() - start;
    }

    @Override protected long readData(String tableName) {
        long start = System.currentTimeMillis();
        // collect() forces full materialization - all rows are read from S3 and pulled to driver
        // Set batch_size=5000 to match Iceberg's default Parquet vectorization batch size
        Row[] rows = (Row[]) spark.read()
            .option("batch_size", "5000")
            .table(tableName)
            .select(col("embedding"))
            .collect();
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
            .config("spark.sql.catalog.lance.root", "s3://" + config.getS3Bucket() + "/" + config.getS3Prefix())
            .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }
}
