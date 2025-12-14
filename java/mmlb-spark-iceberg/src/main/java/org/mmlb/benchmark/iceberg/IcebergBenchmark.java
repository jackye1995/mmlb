/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.mmlb.benchmark.iceberg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.mmlb.benchmark.BenchmarkConfig;
import org.mmlb.benchmark.SparkBenchmarkBase;
import static org.apache.spark.sql.functions.col;

public class IcebergBenchmark extends SparkBenchmarkBase {
    public IcebergBenchmark(BenchmarkConfig config) { super(config); }

    @Override protected String getFormatName() { return "Iceberg"; }
    @Override protected String getTableName(String baseName) { return "iceberg.benchmark." + baseName + tableSuffix; }
    @Override protected void cleanupTable(String tableName) { try { spark.sql("DROP TABLE IF EXISTS " + tableName); } catch (Exception e) { } }

    @Override protected long writeData(Dataset<Row> data, String tableName) {
        cleanupTable(tableName);
        long start = System.currentTimeMillis();
        try {
            data.coalesce(1)
                .writeTo(tableName)
                .tableProperty("write.target-file-size-bytes", "10737418240") // 10GB - ensures single file output
                .create();
        } catch (org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException e) {
            throw new RuntimeException("Table already exists after cleanup: " + tableName, e);
        }
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
        // Use s3:// scheme with FileIOCatalog and S3FileIO (no Hadoop FileSystem dependency)
        String warehousePath = "s3://" + config.getS3Bucket() + "/" + config.getS3Prefix() + "/iceberg-warehouse";
        spark = createBaseBuilder()
            // Use our custom FileIOCatalog that works with S3FileIO directly
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.catalog-impl", "org.mmlb.benchmark.iceberg.FileIOCatalog")
            .config("spark.sql.catalog.iceberg.warehouse", warehousePath)
            // Use S3FileIO for all file operations (no Hadoop dependency)
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.benchmark");
    }
}
