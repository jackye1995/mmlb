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

import java.util.UUID;

public class BenchmarkConfig {
    public static final int DEFAULT_EMBEDDING_DIM = 1024;
    public static final int DEFAULT_NUM_ROWS = 500_000;
    public static final int DEFAULT_BATCH_SIZE = 1024;
    public static final int DEFAULT_MAX_ROWS_PER_FILE = 500_000;
    public static final int DEFAULT_WARMUP_ITERATIONS = 1;
    public static final int DEFAULT_MEASURE_ITERATIONS = 3;
    public static final String DEFAULT_S3_BUCKET = "jack-lancedb-devland-us-east-1";
    public static final int DEFAULT_LANCE_UPLOAD_CONCURRENCY = 40;
    public static final int DEFAULT_LANCE_INITIAL_UPLOAD_SIZE = 20971520;

    private final int embeddingDim;
    private final int numRows;
    private final int batchSize;
    private final int maxRowsPerFile;
    private final int warmupIterations;
    private final int measureIterations;
    private final String s3Bucket;
    private final String s3Prefix;
    private final int lanceUploadConcurrency;
    private final int lanceInitialUploadSize;
    private final String configName;

    public BenchmarkConfig() {
        this.embeddingDim = getEnvInt("BENCHMARK_EMBEDDING_DIM", DEFAULT_EMBEDDING_DIM);
        this.numRows = getEnvInt("BENCHMARK_NUM_ROWS", DEFAULT_NUM_ROWS);
        this.batchSize = getEnvInt("BENCHMARK_BATCH_SIZE", DEFAULT_BATCH_SIZE);
        this.maxRowsPerFile = getEnvInt("BENCHMARK_MAX_ROWS_PER_FILE", DEFAULT_MAX_ROWS_PER_FILE);
        this.warmupIterations = getEnvInt("BENCHMARK_WARMUP_ITERATIONS", DEFAULT_WARMUP_ITERATIONS);
        this.measureIterations = getEnvInt("BENCHMARK_MEASURE_ITERATIONS", DEFAULT_MEASURE_ITERATIONS);
        this.s3Bucket = getEnvString("BENCHMARK_S3_BUCKET", DEFAULT_S3_BUCKET);
        this.s3Prefix = getEnvString("BENCHMARK_S3_PREFIX",
                "spark-benchmark-" + System.currentTimeMillis() + "-" + UUID.randomUUID());
        this.lanceUploadConcurrency = getEnvInt("LANCE_UPLOAD_CONCURRENCY", DEFAULT_LANCE_UPLOAD_CONCURRENCY);
        this.lanceInitialUploadSize = getEnvInt("LANCE_INITIAL_UPLOAD_SIZE", DEFAULT_LANCE_INITIAL_UPLOAD_SIZE);
        this.configName = getEnvString("BENCHMARK_CONFIG_NAME", "default");
    }

    private static int getEnvInt(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value != null && !value.isEmpty()) {
            try { return Integer.parseInt(value); }
            catch (NumberFormatException e) { }
        }
        return defaultValue;
    }

    private static String getEnvString(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }

    public int getEmbeddingDim() { return embeddingDim; }
    public int getNumRows() { return numRows; }
    public int getBatchSize() { return batchSize; }
    public int getMaxRowsPerFile() { return maxRowsPerFile; }
    public int getWarmupIterations() { return warmupIterations; }
    public int getMeasureIterations() { return measureIterations; }
    public String getS3Bucket() { return s3Bucket; }
    public String getS3Prefix() { return s3Prefix; }
    public int getLanceUploadConcurrency() { return lanceUploadConcurrency; }
    public int getLanceInitialUploadSize() { return lanceInitialUploadSize; }
    public String getConfigName() { return configName; }

    @Override
    public String toString() {
        return String.format("BenchmarkConfig{configName=%s, numRows=%,d, embeddingDim=%d}",
                configName, numRows, embeddingDim);
    }
}
