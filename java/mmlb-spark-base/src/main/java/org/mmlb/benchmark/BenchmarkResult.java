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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BenchmarkResult {
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private String formatName;
    private String benchmarkType;
    private String timestamp;
    private String s3Prefix;
    private String configName;
    private int numRows;
    private int embeddingDim;
    private Integer lanceUploadConcurrency;
    private Integer lanceInitialUploadSize;
    private List<Long> writeTimes;
    private Double avgWriteTimeMs;
    private Double writeThroughput;
    private List<Long> readTimes;
    private Double avgReadTimeMs;
    private Double readThroughput;
    private Long sourceWriteTimeMs;

    public BenchmarkResult() {
        this.timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        this.writeTimes = new ArrayList<>();
        this.readTimes = new ArrayList<>();
    }

    public BenchmarkResult(String formatName, String benchmarkType) {
        this();
        this.formatName = formatName;
        this.benchmarkType = benchmarkType;
    }

    public void calculateAverages(int numRows) {
        this.numRows = numRows;
        if (!writeTimes.isEmpty()) {
            avgWriteTimeMs = writeTimes.stream().mapToLong(l -> l).average().orElse(0);
            writeThroughput = (double) numRows / avgWriteTimeMs * 1000;
        }
        if (!readTimes.isEmpty()) {
            avgReadTimeMs = readTimes.stream().mapToLong(l -> l).average().orElse(0);
            readThroughput = (double) numRows / avgReadTimeMs * 1000;
        }
    }

    public String toJson() {
        try { return MAPPER.writeValueAsString(this); }
        catch (IOException e) { throw new RuntimeException("Failed to serialize", e); }
    }

    // Getters and setters
    public String getFormatName() { return formatName; }
    public void setFormatName(String v) { this.formatName = v; }
    public String getBenchmarkType() { return benchmarkType; }
    public void setBenchmarkType(String v) { this.benchmarkType = v; }
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String v) { this.timestamp = v; }
    public String getS3Prefix() { return s3Prefix; }
    public void setS3Prefix(String v) { this.s3Prefix = v; }
    public int getNumRows() { return numRows; }
    public void setNumRows(int v) { this.numRows = v; }
    public int getEmbeddingDim() { return embeddingDim; }
    public void setEmbeddingDim(int v) { this.embeddingDim = v; }
    public String getConfigName() { return configName; }
    public void setConfigName(String v) { this.configName = v; }
    public Integer getLanceUploadConcurrency() { return lanceUploadConcurrency; }
    public void setLanceUploadConcurrency(Integer v) { this.lanceUploadConcurrency = v; }
    public Integer getLanceInitialUploadSize() { return lanceInitialUploadSize; }
    public void setLanceInitialUploadSize(Integer v) { this.lanceInitialUploadSize = v; }
    public List<Long> getWriteTimes() { return writeTimes; }
    public void setWriteTimes(List<Long> v) { this.writeTimes = v; }
    public void addWriteTime(long time) { this.writeTimes.add(time); }
    public Double getAvgWriteTimeMs() { return avgWriteTimeMs; }
    public void setAvgWriteTimeMs(Double v) { this.avgWriteTimeMs = v; }
    public Double getWriteThroughput() { return writeThroughput; }
    public void setWriteThroughput(Double v) { this.writeThroughput = v; }
    public List<Long> getReadTimes() { return readTimes; }
    public void setReadTimes(List<Long> v) { this.readTimes = v; }
    public void addReadTime(long time) { this.readTimes.add(time); }
    public Double getAvgReadTimeMs() { return avgReadTimeMs; }
    public void setAvgReadTimeMs(Double v) { this.avgReadTimeMs = v; }
    public Double getReadThroughput() { return readThroughput; }
    public void setReadThroughput(Double v) { this.readThroughput = v; }
    public Long getSourceWriteTimeMs() { return sourceWriteTimeMs; }
    public void setSourceWriteTimeMs(Long v) { this.sourceWriteTimeMs = v; }
}
