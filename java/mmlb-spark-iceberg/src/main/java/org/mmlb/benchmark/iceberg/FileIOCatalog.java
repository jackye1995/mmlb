/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.mmlb.benchmark.iceberg;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;

/**
 * A simple Iceberg catalog implementation that uses FileIO for all operations.
 * This catalog does not require Hadoop FileSystem and can work directly with S3FileIO.
 *
 * Unlike HadoopCatalog, this implementation:
 * - Uses only FileIO for reading/writing metadata files
 * - Does not require atomic rename for commits (overwrites version-hint.text)
 * - Stores table metadata locations using version-hint.text files
 */
public class FileIOCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Closeable {

    private String catalogName;
    private String warehouseLocation;
    private FileIO fileIO;
    private CloseableGroup closeableGroup;
    private Map<String, String> catalogProperties;

    // In-memory namespace tracking (namespaces are implicit in table paths)
    private final ConcurrentMap<Namespace, Map<String, String>> namespaces = new ConcurrentHashMap<>();

    public FileIOCatalog() {}

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        this.catalogName = name != null ? name : FileIOCatalog.class.getSimpleName();
        this.catalogProperties = properties;

        String warehouse = properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");
        this.warehouseLocation = warehouse.replaceAll("/*$", "");

        // Initialize FileIO from properties (supports S3FileIO, etc.)
        this.fileIO = CatalogUtil.loadFileIO(
            properties.getOrDefault(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO"),
            properties,
            null
        );

        this.closeableGroup = new CloseableGroup();
        closeableGroup.addCloseable(metricsReporter());
        closeableGroup.addCloseable(fileIO);
        closeableGroup.setSuppressCloseFailure(true);
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        String tableLocation = defaultWarehouseLocation(tableIdentifier);
        return new FileIOTableOperations(tableLocation, fileIO, fullTableName(catalogName, tableIdentifier));
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        StringBuilder sb = new StringBuilder(warehouseLocation);
        for (String level : tableIdentifier.namespace().levels()) {
            sb.append("/").append(level);
        }
        sb.append("/").append(tableIdentifier.name());
        return sb.toString();
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        // This simple implementation doesn't support listing
        // For a full implementation, you'd need to list files in the namespace directory
        throw new UnsupportedOperationException("Table listing not supported in FileIOCatalog");
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
        TableOperations ops = newTableOps(tableIdentifier);
        TableMetadata lastMetadata = null;

        try {
            lastMetadata = ops.current();
        } catch (Exception e) {
            // Table doesn't exist
            return false;
        }

        if (lastMetadata == null) {
            return false;
        }

        if (purge) {
            CatalogUtil.dropTableData(ops.io(), lastMetadata);
        }

        // Delete the version-hint file to "drop" the table
        String versionHintPath = defaultWarehouseLocation(tableIdentifier) + "/metadata/version-hint.text";
        try {
            ops.io().deleteFile(versionHintPath);
        } catch (Exception e) {
            // Ignore if file doesn't exist
        }

        return true;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        throw new UnsupportedOperationException("Table renaming not supported in FileIOCatalog");
    }

    // SupportsNamespaces implementation

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
        if (namespaces.containsKey(namespace)) {
            throw new AlreadyExistsException("Namespace already exists: %s", namespace);
        }
        namespaces.put(namespace, metadata);
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        return namespaces.keySet().stream()
            .filter(n -> namespace.isEmpty() || n.toString().startsWith(namespace.toString()))
            .collect(Collectors.toList());
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        Map<String, String> metadata = namespaces.get(namespace);
        if (metadata == null) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        return metadata;
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        return namespaces.remove(namespace) != null;
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
        if (!namespaces.containsKey(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        namespaces.get(namespace).putAll(properties);
        return true;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
        if (!namespaces.containsKey(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        properties.forEach(p -> namespaces.get(namespace).remove(p));
        return true;
    }

    @Override
    public void close() throws IOException {
        closeableGroup.close();
    }

    @Override
    protected Map<String, String> properties() {
        return catalogProperties;
    }
}
