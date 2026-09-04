/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.ExpressionConverter;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tier-1 validation (see memory {@code iceberg_pruning_perf_validation} / GitHub issue #10818):
 * {@link VisibilityDomainPruningTest} and {@code VisibilityPruningIntegrationTest} only assert
 * that {@link VisibilityDomainPruning} builds the right {@code Domain}/{@code Constraint} object
 * and that it's wired into {@code SpatialConnectorMetadata.applyFilter()} correctly — neither
 * proves that domain ever causes Iceberg to actually drop a file.
 *
 * <p>This test closes that gap without needing a running Trino query engine. It feeds a
 * {@code VisibilityDomainPruning} domain through {@link ExpressionConverter}
 * ({@code io.trino.plugin.iceberg.ExpressionConverter}) — the exact class Trino's real Iceberg
 * connector metadata uses to turn a pushed-down {@code TupleDomain} into an Iceberg
 * {@code Expression} — and then applies that expression to a real Iceberg {@link Table} (backed
 * by {@link InMemoryCatalog}, Iceberg's own dependency-free in-memory catalog meant for exactly
 * this kind of test) whose data files carry hand-specified, skewed {@code __vis__} column
 * statistics (real {@link Metrics}/{@link DataFile} objects, nothing mocked — only the underlying
 * bytes are fake, which is fine since manifest-level pruning never reads them). Asserting on
 * {@code TableScan.planFiles()} exercises Iceberg's actual manifest evaluator, proving the
 * mechanism — not just the plumbing — works.
 *
 * <p>What this does NOT prove: an end-to-end wall-clock speedup, or that Trino's own
 * {@code IcebergMetadata.applyFilter()} threads the domain through identically in a live query
 * (that requires the tier-2 {@code DistributedQueryRunner} benchmark described in the
 * {@code iceberg_pruning_perf_validation} memory).
 */
class IcebergVisibilityPruningMechanismTest {

    private static final int ID_FIELD = 1;
    private static final int VIS_FIELD = 2;

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(ID_FIELD, "id", Types.IntegerType.get()),
        Types.NestedField.optional(VIS_FIELD, "__vis__", Types.StringType.get()));

    private static final IcebergColumnHandle VIS_COLUMN_HANDLE = new IcebergColumnHandle(
        ColumnIdentity.primitiveColumnIdentity(VIS_FIELD, "__vis__"),
        VarcharType.VARCHAR,
        List.of(),
        VarcharType.VARCHAR,
        true,
        Optional.empty());

    private Table table;

    @BeforeEach
    void setUp() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        catalog.createNamespace(Namespace.of("ns"));
        table = catalog.createTable(TableIdentifier.of("ns", "vis_table"), SCHEMA, PartitionSpec.unpartitioned());
    }

    @Test
    void noFilterScansEveryFile() throws IOException {
        appendFile("admin", 1000, false);
        appendFile("ops", 1000, false);
        appendFile(null, 1000, true);

        assertThat(scannedFileCount(Optional.empty())).isEqualTo(3);
    }

    @Test
    void emptyAuthsDomainPrunesEveryFileWithoutNulls() throws IOException {
        // Restricted files (no NULL visibility values) + one unrestricted (all-NULL) file.
        appendFile("admin", 1000, false);
        appendFile("admin", 1000, false);
        appendFile("ops", 1000, false);
        appendFile(null, 1000, true);

        Domain domain = VisibilityDomainPruning.emptyAuthsDomain(VarcharType.VARCHAR, Set.of()).orElseThrow();
        Expression expr = toIcebergExpression(domain);

        // A caller with no authorizations can only ever see NULL visibility rows (see
        // VisibilityDomainPruning javadoc); only the all-NULL file has any candidate rows.
        assertThat(scannedFileCount(Optional.of(expr))).isEqualTo(1);
    }

    @Test
    void expressionDomainPrunesFilesHoldingOnlyDisjointExpressions() throws IOException {
        appendFile("admin", 1000, false);
        appendFile("ops", 1000, false);
        appendFile("finance", 1000, false);
        appendFile(null, 1000, true);

        // Declared universe of every non-null value present; caller holds only "ops".
        Domain domain = VisibilityDomainPruning.expressionDomain(
            VarcharType.VARCHAR, Set.of("admin", "ops", "finance"), Set.of("ops")).orElseThrow();
        Expression expr = toIcebergExpression(domain);

        // The "ops" file and the all-NULL (unrestricted) file survive; "admin" and "finance"
        // hold only values the caller's auths cannot satisfy, so they're pruned.
        assertThat(scannedFileCount(Optional.of(expr))).isEqualTo(2);
    }

    @Test
    void expressionDomainDoesNotPruneMixedFile() throws IOException {
        // A single file whose min/max span both an admissible and an inadmissible value:
        // Iceberg's manifest evaluator can only compare against [min, max], so it cannot exclude
        // this file even though only one of the values present is visible to the caller.
        // Demonstrates the "narrows, never widens" safety property from VisibilityDomainPruning's
        // javadoc: pruning misses this file (a missed optimization) rather than wrongly dropping it.
        appendFileWithBounds("admin", "ops", 1000);

        Domain domain = VisibilityDomainPruning.expressionDomain(
            VarcharType.VARCHAR, Set.of("admin", "ops"), Set.of("ops")).orElseThrow();
        Expression expr = toIcebergExpression(domain);

        assertThat(scannedFileCount(Optional.of(expr))).isEqualTo(1);
    }

    private static Expression toIcebergExpression(Domain domain) {
        return ExpressionConverter.toIcebergExpression(
            TupleDomain.withColumnDomains(Map.of(VIS_COLUMN_HANDLE, domain)));
    }

    private void appendFile(String visValue, long rowCount, boolean allNull) {
        appendFileWithBounds(allNull ? null : visValue, allNull ? null : visValue, rowCount);
    }

    /** Registers a file whose visibility column's [min, max] manifest bounds are exactly as given. */
    private void appendFileWithBounds(String lowerVisValue, String upperVisValue, long rowCount) {
        // No real bytes are written or read: planFiles() only consults manifest-level Metrics,
        // so a bare path placeholder is enough to exercise the pruning mechanism.
        String dataFilePath = "memory:/ns/vis_table/data/data-" + UUID.randomUUID() + ".parquet";

        boolean allNull = lowerVisValue == null && upperVisValue == null;

        Map<Integer, Long> valueCounts = new HashMap<>();
        valueCounts.put(ID_FIELD, rowCount);
        valueCounts.put(VIS_FIELD, rowCount);

        Map<Integer, Long> nullCounts = new HashMap<>();
        nullCounts.put(ID_FIELD, 0L);
        nullCounts.put(VIS_FIELD, allNull ? rowCount : 0L);

        Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
        Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
        if (!allNull) {
            lowerBounds.put(VIS_FIELD, Conversions.toByteBuffer(Types.StringType.get(), lowerVisValue));
            upperBounds.put(VIS_FIELD, Conversions.toByteBuffer(Types.StringType.get(), upperVisValue));
        }

        Metrics metrics = new Metrics(rowCount, null, valueCounts, nullCounts, null, lowerBounds, upperBounds);

        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(dataFilePath)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(1024L)
            .withMetrics(metrics)
            .build();

        table.newAppend().appendFile(file).commit();
    }

    private long scannedFileCount(Optional<Expression> filter) throws IOException {
        try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
                 filter.map(e -> table.newScan().filter(e)).orElseGet(table::newScan).planFiles()) {
            long count = 0;
            for (org.apache.iceberg.FileScanTask ignored : tasks) {
                count++;
            }
            return count;
        }
    }
}
