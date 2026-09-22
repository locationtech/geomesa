/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.SpatialIcebergPlugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tier-2 end-to-end validation of visibility-column file pruning (GitHub issue #10818),
 * the piece {@link org.locationtech.geomesa.trino.security.IcebergVisibilityPruningMechanismTest}
 * explicitly does <em>not</em> cover: that a real Trino query, planned and executed by the actual
 * {@code IcebergMetadata.applyFilter} through a live {@link DistributedQueryRunner}, drops whole
 * data files based on the visibility domain this connector injects.
 *
 * <p><strong>What we assert, and why it is not a wall-clock benchmark.</strong> Timing is flaky
 * and proves nothing deterministic in CI. The causally-correct, deterministic signal that file
 * pruning happened is how many rows the query <em>physically read from storage</em>
 * ({@link QueryStats#getPhysicalInputPositions()}). File/manifest pruning happens at split
 * generation, so a pruned file yields no splits and contributes zero physical input; the
 * always-on {@code is_visible()} row filter runs <em>after</em> the scan and therefore never
 * changes this count. So physical input positions isolate file pruning from row filtering.
 *
 * <p><strong>Layout.</strong> The table is written one visibility value per data file (each
 * {@code INSERT} is its own Iceberg commit → its own file), 100 rows each, so every file's
 * {@code __vis__} min == max — the skew that makes manifest min/max pruning observable. Four
 * files: {@code admin}, {@code ops}, {@code finance}, and one all-{@code NULL} file. A NULL (or
 * empty) visibility carries no real expression, so it is an anomaly hidden from everyone and its
 * file is pruned for <em>every</em> caller.
 *
 * <p><strong>Differential.</strong> A single catalog and table, varying only the session
 * identity, so the reduction is attributable to pruning and nothing else:
 * <ul>
 *   <li>all-auths user → {@code expressionDomain} admits admin/ops/finance (never NULL) → 3
 *       files → 300 rows (the widest a caller can read);</li>
 *   <li>ops-only user → admits {@code ops} only (NULL never admitted) → 1 file → 100 rows;</li>
 *   <li>no-auths user → {@code emptyAuthsDomain} is {@code Domain.none} → every file pruned →
 *       0 rows.</li>
 * </ul>
 *
 * <p>Tagged {@code integration} and named {@code *IT} so the fast surefire lane skips it (see this
 * module's {@code excludedGroups=integration}) and Failsafe runs it under {@code -DskipITs=false}.
 */
@Tag("integration")
class VisibilityPruningEndToEndIT {

    // The spatial_iceberg connector is a read-path wrapper and deliberately does not support
    // writes (in production, tables are created and populated by external GeoMesa ingest, not
    // Trino DDL). So we register two catalogs over the *same* TESTING_FILE_METASTORE warehouse:
    // a plain iceberg catalog to CREATE/INSERT the fixture, and the spatial_iceberg catalog to
    // run the reads whose file pruning we measure. Same physical Iceberg table, two views of it.
    private static final String READ_CATALOG = "spatial";   // spatial_iceberg — applies vis pruning
    private static final String WRITE_CATALOG = "ice";       // plain iceberg — fixture setup only
    private static final String SCHEMA = "vis";
    private static final String READ_TABLE = READ_CATALOG + "." + SCHEMA + ".t";
    private static final String WRITE_TABLE = WRITE_CATALOG + "." + SCHEMA + ".t";
    private static final int ROWS_PER_FILE = 100;

    private static DistributedQueryRunner runner;

    @BeforeAll
    static void setUp(@org.junit.jupiter.api.io.TempDir Path tmp) throws Exception {
        // Identity → auth-token mapping consumed by the built-in FileAuthorizationResolver.
        // 'nobody' is deliberately absent → resolves to the empty auth set (fail-closed).
        Path authFile = tmp.resolve("auths.properties");
        Files.writeString(authFile, String.join("\n",
            "user.allauths=admin,ops,finance",
            "user.opsuser=ops"));

        Path warehouse = tmp.resolve("warehouse");
        Files.createDirectories(warehouse);

        // Self-contained, dependency-free iceberg catalog on the local filesystem. Shared by
        // both catalogs so the fixture written through 'ice' is read back through 'spatial'.
        Map<String, String> icebergConfig = Map.of(
            "iceberg.catalog.type", "TESTING_FILE_METASTORE",
            "hive.metastore.catalog.dir", warehouse.toUri().toString(),
            "fs.hadoop.enabled", "true");

        Session defaultSession = testSessionBuilder()
            .setCatalog(READ_CATALOG)
            .setSchema(SCHEMA)
            .build();

        runner = DistributedQueryRunner.builder(defaultSession)
            .setWorkerCount(1)
            .build();

        runner.installPlugin(new IcebergPlugin());
        runner.createCatalog(WRITE_CATALOG, "iceberg", icebergConfig);

        runner.installPlugin(new SpatialIcebergPlugin());
        Map<String, String> spatialConfig = new java.util.LinkedHashMap<>(icebergConfig);
        // Trino-layer visibility enforcement + the sound expression-domain pruning tier.
        spatialConfig.put("geomesa.security.auth-resolver", "file");
        spatialConfig.put("geomesa.security.auth-mapping-file", authFile.toString());
        spatialConfig.put("geomesa.security.enable-visibility-expression-pruning", "true");
        spatialConfig.put("geomesa.security.visibility-expressions", "admin,ops,finance");
        runner.createCatalog(READ_CATALOG, "spatial_iceberg", spatialConfig);

        // Write the fixture through the plain iceberg catalog.
        runner.execute("CREATE SCHEMA IF NOT EXISTS " + WRITE_CATALOG + "." + SCHEMA);
        runner.execute("CREATE TABLE " + WRITE_TABLE + " (id bigint, \"__vis__\" varchar)");
        // One INSERT per visibility value ⇒ one data file each, min == max on __vis__.
        appendFile("admin");
        appendFile("ops");
        appendFile("finance");
        runner.execute("INSERT INTO " + WRITE_TABLE
            + " SELECT x, CAST(NULL AS varchar) FROM UNNEST(sequence(1, " + ROWS_PER_FILE + ")) AS u(x)");
    }

    private static void appendFile(String vis) {
        runner.execute("INSERT INTO " + WRITE_TABLE + " SELECT x, '" + vis + "'"
            + " FROM UNNEST(sequence(1, " + ROWS_PER_FILE + ")) AS u(x)");
    }

    @AfterAll
    static void tearDown() {
        if (runner != null) {
            runner.close();
        }
    }

    @Test
    void allAuthsUserReadsEveryNonNullFile() {
        // Widest read: expressionDomain admits admin/ops/finance but never NULL, so the three
        // real-expression files survive and the all-NULL file is pruned.
        assertThat(physicalRowsRead("allauths")).isEqualTo(3L * ROWS_PER_FILE);
    }

    @Test
    void opsOnlyUserReadsOnlyOpsFile() {
        // admin & finance files hold unsatisfiable values; the all-NULL file is a hidden anomaly.
        // Only the ops file survives.
        assertThat(physicalRowsRead("opsuser")).isEqualTo(ROWS_PER_FILE);
    }

    @Test
    void noAuthsUserReadsNothing() {
        // emptyAuthsDomain is Domain.none: a no-auth caller can see no rows (NULL/empty are hidden
        // anomalies and no expression is satisfiable), so every file is pruned.
        assertThat(physicalRowsRead("nobody")).isEqualTo(0L);
    }

    @Test
    void prunedUserReadsStrictlyLessThanBaseline() {
        // Guards the causal claim directly: less data is physically read when auths are narrower.
        long baseline = physicalRowsRead("allauths");
        assertThat(physicalRowsRead("opsuser")).isLessThan(baseline);
        assertThat(physicalRowsRead("nobody")).isLessThan(baseline);
    }

    /**
     * Runs {@code SELECT id FROM t} (a full scan that must read rows — chosen over
     * {@code count(*)}, which iceberg can answer from metadata without a scan) as the given user,
     * and returns the rows physically read from storage for that query.
     */
    private long physicalRowsRead(String user) {
        Session session = testSessionBuilder()
            .setCatalog(READ_CATALOG)
            .setSchema(SCHEMA)
            .setIdentity(Identity.forUser(user).build())
            .build();
        QueryRunner.MaterializedResultWithPlan result =
            runner.executeWithPlan(session, "SELECT id FROM " + READ_TABLE);
        QueryStats stats = runner.getCoordinator().getQueryManager()
            .getFullQueryInfo(result.queryId()).getQueryStats();
        return stats.getPhysicalInputPositions();
    }
}
