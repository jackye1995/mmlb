/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mmlb.benchmark;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.*;
import scala.collection.mutable.WrappedArray;
import java.io.*;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

public abstract class SparkBenchmarkBase {
    protected final BenchmarkConfig config;
    protected SparkSession spark;
    protected final String tableSuffix;
    private StringBuilder logBuffer;
    private String currentBenchmarkName;

    public SparkBenchmarkBase(BenchmarkConfig config) {
        this.config = config;
        this.tableSuffix = "_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    protected abstract String getFormatName();
    protected abstract String getTableName(String baseName);
    protected abstract void cleanupTable(String tableName);
    protected abstract long writeData(Dataset<Row> data, String tableName);
    protected abstract long readData(String tableName);
    protected abstract void initializeSpark();

    protected void log(String message) {
        System.out.println(message);
        if (logBuffer != null) logBuffer.append(message).append("\n");
    }

    protected void logf(String format, Object... args) {
        String message = String.format(format, args);
        System.out.print(message);
        if (logBuffer != null) logBuffer.append(message);
    }

    private void initLog(String benchmarkName) {
        this.currentBenchmarkName = benchmarkName;
        this.logBuffer = new StringBuilder();
        logBuffer.append("Benchmark: ").append(benchmarkName).append("\n");
        logBuffer.append("Timestamp: ").append(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)).append("\n\n");
    }

    private void saveAndUploadLog(BenchmarkResult result) {
        if (logBuffer == null) return;
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String baseName = String.format("%s_%s_%s", getFormatName().toLowerCase(), currentBenchmarkName.toLowerCase().replace(" ", "_"), timestamp);

        File logFile = new File(System.getProperty("java.io.tmpdir"), baseName + ".log");
        File jsonFile = new File(System.getProperty("java.io.tmpdir"), baseName + ".json");
        try (PrintWriter w = new PrintWriter(new FileWriter(logFile))) { w.print(logBuffer.toString()); } catch (IOException e) { }
        try (PrintWriter w = new PrintWriter(new FileWriter(jsonFile))) { w.print(result.toJson()); } catch (IOException e) { }

        try {
            AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
            s3.putObject(config.getS3Bucket(), config.getS3Prefix() + "/logs/" + baseName + ".log", logFile);
            s3.putObject(config.getS3Bucket(), config.getS3Prefix() + "/results/" + baseName + ".json", jsonFile);
            log("--- Uploaded to S3: s3://" + config.getS3Bucket() + "/" + config.getS3Prefix() + "/ ---");
        } catch (Exception e) { System.err.println("S3 upload failed: " + e.getMessage()); }
    }

    protected SparkSession.Builder createBaseBuilder() {
        return SparkSession.builder().appName(getClass().getSimpleName()).master("local[*]")
                .config("spark.driver.memory", "8g").config("spark.sql.shuffle.partitions", "4")
                .config("spark.executor.heartbeatInterval", "300s").config("spark.network.timeout", "3600s");
    }

    protected void registerDataGenerationUDFs() {
        final int dim = config.getEmbeddingDim();
        spark.udf().register("gen_timestamp", (UDF1<Long, Timestamp>) id -> new Timestamp(System.currentTimeMillis() - new Random(id + 1000).nextInt(86400000)), DataTypes.TimestampType);
        String[] labels = {"cat_a", "cat_b", "cat_c", "cat_d", "cat_e"};
        spark.udf().register("gen_label", (UDF1<Long, String>) id -> labels[new Random(id + 2000).nextInt(labels.length)], DataTypes.StringType);
        spark.udf().register("gen_score", (UDF1<Long, Double>) id -> new Random(id + 3000).nextDouble() * 100, DataTypes.DoubleType);
        spark.udf().register("gen_embedding", (UDF1<Long, WrappedArray<Float>>) id -> {
            Random r = new Random(id + 4000); Float[] emb = new Float[dim];
            for (int j = 0; j < dim; j++) emb[j] = r.nextFloat();
            return WrappedArray.make(emb);
        }, new ArrayType(DataTypes.FloatType, false));
    }

    protected StructType getSchema() {
        Metadata meta = new MetadataBuilder().putLong("arrow.fixed-size-list.size", config.getEmbeddingDim()).build();
        return new StructType(new StructField[]{
            new StructField("id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("label", DataTypes.StringType, true, Metadata.empty()),
            new StructField("score", DataTypes.DoubleType, true, Metadata.empty()),
            new StructField("embedding", new ArrayType(DataTypes.FloatType, false), true, meta)
        });
    }

    protected Dataset<Row> generateData(int numRows) {
        Dataset<Row> data = spark.range(numRows).selectExpr("id", "gen_timestamp(id) as timestamp", "gen_label(id) as label", "gen_score(id) as score", "gen_embedding(id) as embedding");
        return spark.createDataFrame(data.javaRDD(), getSchema());
    }

    public BenchmarkResult runBenchmark(int writeIter, int readIter) {
        String type = (writeIter > 0 && readIter > 0) ? "Read_Write" : (writeIter > 0) ? "Write" : "Read";
        initLog(type);
        BenchmarkResult result = new BenchmarkResult(getFormatName(), type);
        result.setS3Prefix(config.getS3Prefix()); result.setNumRows(config.getNumRows());
        result.setEmbeddingDim(config.getEmbeddingDim()); result.setConfigName(config.getConfigName());
        result.setLanceUploadConcurrency(config.getLanceUploadConcurrency());
        result.setLanceInitialUploadSize(config.getLanceInitialUploadSize());

        log("=".repeat(70)); logf("%s %s Benchmark (S3)%n", getFormatName(), type); log("=".repeat(70));
        logf("Rows: %,d, Embedding: %d%n", config.getNumRows(), config.getEmbeddingDim());

        Dataset<Row> data = generateData(config.getNumRows());
        String sourceTable = getTableName("source_" + type.toLowerCase());

        if (readIter > 0 || writeIter == 0) {
            log("\n--- Writing source data ---");
            long t = writeData(data, sourceTable); result.setSourceWriteTimeMs(t); logf("Source write: %,d ms%n", t);
        }

        log("\n--- Warmup ---");
        if (writeIter > 0) { logf("Warmup write: %,d ms%n", writeData(data, getTableName("warmup"))); }
        if (readIter > 0) { for (int i = 0; i < config.getWarmupIterations(); i++) logf("Warmup read %d: %,d ms%n", i+1, readData(sourceTable)); }

        if (writeIter > 0) { log("\n--- Writes ---"); for (int i = 0; i < writeIter; i++) { long t = writeData(data, getTableName("bench_" + i)); result.addWriteTime(t); logf("Write %d: %,d ms%n", i+1, t); } }
        if (readIter > 0) { log("\n--- Reads ---"); for (int i = 0; i < readIter; i++) { long t = readData(sourceTable); result.addReadTime(t); logf("Read %d: %,d ms%n", i+1, t); } }

        result.calculateAverages(config.getNumRows());
        log("\n" + "=".repeat(70)); logf("%s SUMMARY%n", getFormatName().toUpperCase()); log("=".repeat(70));
        if (result.getAvgWriteTimeMs() != null) logf("Avg write: %.2f ms, Throughput: %.2f rows/sec%n", result.getAvgWriteTimeMs(), result.getWriteThroughput());
        if (result.getAvgReadTimeMs() != null) logf("Avg read: %.2f ms, Throughput: %.2f rows/sec%n", result.getAvgReadTimeMs(), result.getReadThroughput());
        saveAndUploadLog(result);
        return result;
    }

    public BenchmarkResult runWriteBenchmark() { return runBenchmark(config.getMeasureIterations(), 0); }
    public BenchmarkResult runReadBenchmark() { return runBenchmark(0, config.getMeasureIterations()); }
    public BenchmarkResult runReadWriteBenchmark() { return runBenchmark(config.getMeasureIterations(), config.getMeasureIterations()); }

    public void start() { initializeSpark(); registerDataGenerationUDFs(); }
    public void stop() { if (spark != null) { spark.stop(); spark = null; } }
}
