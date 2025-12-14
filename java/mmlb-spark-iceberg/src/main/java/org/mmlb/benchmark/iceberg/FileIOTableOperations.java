/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.mmlb.benchmark.iceberg;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TableOperations implementation using only FileIO, without Hadoop FileSystem.
 *
 * This implementation:
 * - Uses version-hint.text to track the current metadata version
 * - Writes versioned metadata files (v0.metadata.json, v1.metadata.json, etc.)
 * - Does not use atomic rename - simply overwrites the version-hint file
 *   (acceptable for benchmarking purposes where we don't need strict atomicity)
 */
public class FileIOTableOperations implements TableOperations {
    private static final Logger LOG = LoggerFactory.getLogger(FileIOTableOperations.class);
    private static final String METADATA_FOLDER = "metadata";
    private static final String VERSION_HINT_FILENAME = "version-hint.text";

    private final String tableLocation;
    private final FileIO fileIO;
    private final String fullTableName;

    private volatile TableMetadata currentMetadata = null;
    private volatile Integer version = null;
    private volatile boolean shouldRefresh = true;

    public FileIOTableOperations(String tableLocation, FileIO fileIO, String fullTableName) {
        this.tableLocation = tableLocation;
        this.fileIO = fileIO;
        this.fullTableName = fullTableName;
    }

    @Override
    public TableMetadata current() {
        if (shouldRefresh) {
            return refresh();
        }
        return currentMetadata;
    }

    @Override
    public TableMetadata refresh() {
        int ver = version != null ? version : readVersionHint();

        try {
            String metadataFile = metadataFileLocation(ver);
            InputFile input = fileIO.newInputFile(metadataFile);

            if (version == null && !input.exists() && ver == 0) {
                // Table doesn't exist yet
                return null;
            }

            // Find the latest version by scanning forward
            while (true) {
                String nextFile = metadataFileLocation(ver + 1);
                InputFile nextInput = fileIO.newInputFile(nextFile);
                if (nextInput.exists()) {
                    ver++;
                    metadataFile = nextFile;
                } else {
                    break;
                }
            }

            this.version = ver;
            this.currentMetadata = TableMetadataParser.read(fileIO, metadataFile);
            this.shouldRefresh = false;

            return currentMetadata;
        } catch (Exception e) {
            throw new RuntimeException("Failed to refresh table metadata", e);
        }
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
        if (base != current()) {
            throw new CommitFailedException("Cannot commit changes based on stale table metadata");
        }

        if (base == metadata) {
            LOG.info("Nothing to commit.");
            return;
        }

        int nextVersion = (version != null ? version : 0) + 1;
        String newMetadataFile = metadataFileLocation(nextVersion);

        // Write the new metadata file
        OutputFile output = fileIO.newOutputFile(newMetadataFile);
        TableMetadataParser.overwrite(metadata, output);

        LOG.info("Committed new metadata file: {}", newMetadataFile);

        // Update version hint (best effort, not atomic)
        writeVersionHint(nextVersion);

        // Clean up old metadata files if configured
        CatalogUtil.deleteRemovedMetadataFiles(fileIO, base, metadata);

        this.shouldRefresh = true;
    }

    @Override
    public FileIO io() {
        return fileIO;
    }

    @Override
    public String metadataFileLocation(String fileName) {
        return String.format("%s/%s/%s", tableLocation, METADATA_FOLDER, fileName);
    }

    @Override
    public LocationProvider locationProvider() {
        TableMetadata metadata = current();
        if (metadata == null) {
            return LocationProviders.locationsFor(tableLocation, null);
        }
        return LocationProviders.locationsFor(metadata.location(), metadata.properties());
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
        return new TableOperations() {
            @Override
            public TableMetadata current() {
                return uncommittedMetadata;
            }

            @Override
            public TableMetadata refresh() {
                throw new UnsupportedOperationException("Cannot call refresh on temporary table operations");
            }

            @Override
            public void commit(TableMetadata base, TableMetadata metadata) {
                throw new UnsupportedOperationException("Cannot call commit on temporary table operations");
            }

            @Override
            public FileIO io() {
                return fileIO;
            }

            @Override
            public String metadataFileLocation(String fileName) {
                return FileIOTableOperations.this.metadataFileLocation(fileName);
            }

            @Override
            public LocationProvider locationProvider() {
                return LocationProviders.locationsFor(uncommittedMetadata.location(), uncommittedMetadata.properties());
            }
        };
    }

    private String metadataFileLocation(int version) {
        return String.format("%s/%s/v%d.metadata.json", tableLocation, METADATA_FOLDER, version);
    }

    private int readVersionHint() {
        String versionHintPath = String.format("%s/%s/%s", tableLocation, METADATA_FOLDER, VERSION_HINT_FILENAME);
        InputFile versionHintFile = fileIO.newInputFile(versionHintPath);

        try {
            if (!versionHintFile.exists()) {
                return 0;
            }

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(versionHintFile.newStream(), StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                if (line != null && !line.trim().isEmpty()) {
                    return Integer.parseInt(line.trim());
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to read version hint, starting from 0", e);
        }

        return 0;
    }

    private void writeVersionHint(int version) {
        String versionHintPath = String.format("%s/%s/%s", tableLocation, METADATA_FOLDER, VERSION_HINT_FILENAME);
        OutputFile versionHintFile = fileIO.newOutputFile(versionHintPath);

        try (PrintWriter writer = new PrintWriter(
                new OutputStreamWriter(versionHintFile.createOrOverwrite(), StandardCharsets.UTF_8))) {
            writer.println(version);
        } catch (Exception e) {
            LOG.warn("Failed to write version hint for version {}", version, e);
        }
    }
}
