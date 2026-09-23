/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.jdbc.TrinoResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.datastore.testcontainers.GeoMesaTrinoContainer;
import org.locationtech.geomesa.trino.datastore.testcontainers.IcebergRestContainer;
import org.locationtech.geomesa.trino.datastore.testcontainers.SeaweedFsContainer;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.MountableFile;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tier-2 end-to-end validation of visibility-column file pruning (GitHub issue #10818),
 * the piece {@code IcebergVisibilityPruningMechanismTest} (plugin module) explicitly does
 * <em>not</em> cover: that a real Trino query, planned and executed by the actual
 * {@code IcebergMetadata.applyFilter}, drops whole data files based on the visibility domain this
 * connector injects.
 *
 * <p>This is the containerized successor to the former embedded-{@code DistributedQueryRunner}
 * version that lived in the plugin module. It runs the real packaged plugin inside a stock
 * {@code trinodb/trino} image against a real Iceberg REST catalog and S3 (SeaweedFS), so the
 * plugin module no longer has to depend on {@code trino-testing}.
 *
 * <p><strong>What we assert, and why it is not a wall-clock benchmark.</strong> Timing is flaky
 * and proves nothing deterministic in CI. The causally-correct, deterministic signal that file
 * pruning happened is how many rows the query <em>physically read from storage</em>
 * (the coordinator's {@code queryStats.physicalInputPositions}). File/manifest pruning happens at
 * split generation, so a pruned file yields no splits and contributes zero physical input; the
 * always-on {@code is_visible()} row filter runs <em>after</em> the scan and therefore never
 * changes this count. So physical input positions isolate file pruning from row filtering — a
 * regression that silently disabled pruning would still read every file's rows (and let the row
 * filter drop them), which this count catches but an output-row count would not.
 *
 * <p>Trino's JDBC {@code StatementStats}/{@code QueryStats} expose {@code processedRows} and
 * {@code physicalInputBytes} but not {@code physicalInputPositions}, so we read the exact field
 * from the coordinator's {@code GET /v1/query/&#123;queryId&#125;} REST endpoint, keyed by the
 * query id JDBC hands back ({@link TrinoResultSet#getQueryId()}).
 *
 * <p><strong>Layout.</strong> The table is written one visibility value per data file (each
 * {@code INSERT} is its own Iceberg commit → its own file), 100 rows each, so every file's
 * {@code __vis__} min == max — the skew that makes manifest min/max pruning observable. Four
 * files: {@code admin}, {@code ops}, {@code finance}, and one all-{@code NULL} file. A NULL (or
 * empty) visibility carries no real expression, so it is an anomaly hidden from everyone and its
 * file is pruned for <em>every</em> caller. The {@code spatial_iceberg} connector is a read-path
 * wrapper and does not support writes, so the fixture is created and populated through a plain
 * {@code iceberg} catalog pointed at the <em>same</em> REST catalog + S3 warehouse.
 *
 * <p><strong>Differential.</strong> A single table, varying only the querying identity (a JDBC
 * connection user mapped to auth tokens by the file resolver), so the reduction is attributable to
 * pruning and nothing else:
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

    private static final String READ_CATALOG = "spatial";   // spatial_iceberg — applies vis pruning
    private static final String WRITE_CATALOG = "iceberg";  // plain iceberg — fixture setup only
    private static final String SCHEMA = "vis";
    private static final String READ_TABLE = READ_CATALOG + "." + SCHEMA + ".t";
    private static final String WRITE_TABLE = WRITE_CATALOG + "." + SCHEMA + ".t";
    private static final int ROWS_PER_FILE = 100;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final Network network = Network.newNetwork();

    private static final SeaweedFsContainer s3 =
            new SeaweedFsContainer("admin", "admin")
                    .withNetwork(network)
                    .withNetworkAliases("seaweed");

    private static final IcebergRestContainer iceberg =
            new IcebergRestContainer("admin", "admin")
                    .withNetwork(network)
                    .withNetworkAliases("rest-catalog");

    @SuppressWarnings("resource")
    private static final TrinoContainer trino =
            new GeoMesaTrinoContainer()
                    .withGeoMesaPlugin()
                    .withNetwork(network)
                    .withNetworkAliases("trino")
                    .withFileSystemBind(resource("docker/trino/pruning/iceberg.properties"),
                            "/etc/trino/catalog/iceberg.properties", BindMode.READ_ONLY)
                    .withFileSystemBind(resource("docker/trino/pruning/spatial.properties"),
                            "/etc/trino/catalog/spatial.properties", BindMode.READ_ONLY)
                    .withFileSystemBind(resource("docker/trino/pruning/geomesa-auths.properties"),
                            "/etc/trino/geomesa-auths.properties", BindMode.READ_ONLY);

    private static String resource(String classpath) {
        return MountableFile.forClasspathResource(classpath).getResolvedPath();
    }

    @BeforeAll
    static void setUp() throws Exception {
        s3.start();
        iceberg.start();
        trino.start();

        // Write the fixture through the plain iceberg catalog (any user; no enforcement there).
        try (Connection conn = connect("admin");
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + WRITE_CATALOG + "." + SCHEMA);
            stmt.execute("CREATE TABLE " + WRITE_TABLE + " (id bigint, \"__vis__\" varchar)");
            // One INSERT per visibility value ⇒ one data file each, min == max on __vis__.
            appendFile(stmt, "admin");
            appendFile(stmt, "ops");
            appendFile(stmt, "finance");
            stmt.execute("INSERT INTO " + WRITE_TABLE
                    + " SELECT x, CAST(NULL AS varchar) FROM UNNEST(sequence(1, " + ROWS_PER_FILE + ")) AS u(x)");
        }
    }

    private static void appendFile(Statement stmt, String vis) throws Exception {
        stmt.execute("INSERT INTO " + WRITE_TABLE + " SELECT x, '" + vis + "'"
                + " FROM UNNEST(sequence(1, " + ROWS_PER_FILE + ")) AS u(x)");
    }

    @AfterAll
    static void tearDown() {
        trino.stop();
        iceberg.stop();
        s3.stop();
    }

    @Test
    void allAuthsUserReadsEveryNonNullFile() throws Exception {
        // Widest read: expressionDomain admits admin/ops/finance but never NULL, so the three
        // real-expression files survive and the all-NULL file is pruned.
        assertThat(physicalRowsRead("allauths")).isEqualTo(3L * ROWS_PER_FILE);
    }

    @Test
    void opsOnlyUserReadsOnlyOpsFile() throws Exception {
        // admin & finance files hold unsatisfiable values; the all-NULL file is a hidden anomaly.
        // Only the ops file survives.
        assertThat(physicalRowsRead("opsuser")).isEqualTo((long) ROWS_PER_FILE);
    }

    @Test
    void noAuthsUserReadsNothing() throws Exception {
        // emptyAuthsDomain is Domain.none: a no-auth caller can see no rows (NULL/empty are hidden
        // anomalies and no expression is satisfiable), so every file is pruned.
        assertThat(physicalRowsRead("nobody")).isEqualTo(0L);
    }

    @Test
    void prunedUserReadsStrictlyLessThanBaseline() throws Exception {
        // Guards the causal claim directly: less data is physically read when auths are narrower.
        long baseline = physicalRowsRead("allauths");
        assertThat(physicalRowsRead("opsuser")).isLessThan(baseline);
        assertThat(physicalRowsRead("nobody")).isLessThan(baseline);
    }

    /**
     * Runs {@code SELECT id FROM t} (a full scan that must read rows — chosen over
     * {@code count(*)}, which iceberg can answer from metadata without a scan) as the given user,
     * and returns the rows physically read from storage for that query, read from the
     * coordinator's query stats.
     */
    private long physicalRowsRead(String user) throws Exception {
        String queryId;
        try (Connection conn = connect(user);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM " + READ_TABLE)) {
            while (rs.next()) {
                // fully drain the result set so the scan runs to completion
                rs.getLong(1);
            }
            queryId = rs.unwrap(TrinoResultSet.class).getQueryId();
        }
        return physicalInputPositions(queryId);
    }

    /** Reads {@code queryStats.physicalInputPositions} from the coordinator's REST query info. */
    private long physicalInputPositions(String queryId) throws Exception {
        URI uri = URI.create("http://" + trino.getHost() + ":" + trino.getFirstMappedPort()
                + "/v1/query/" + queryId);
        // The query is finished once the JDBC result set is drained, but final stats can lag the
        // last client fetch by a beat, so poll until the coordinator reports a terminal state.
        for (int attempt = 0; attempt < 50; attempt++) {
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .header("X-Trino-User", "admin")
                    .GET()
                    .build();
            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "GET " + uri + " returned " + response.statusCode() + ": " + response.body());
            }
            JsonNode root = MAPPER.readTree(response.body());
            String state = root.path("state").asText();
            if ("FAILED".equals(state)) {
                throw new IllegalStateException("Query " + queryId + " failed: "
                        + root.path("errorMessage").asText());
            }
            if ("FINISHED".equals(state)) {
                return root.path("queryStats").path("physicalInputPositions").asLong();
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("Query " + queryId + " did not reach a terminal state");
    }

    private static Connection connect(String user) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", user);
        String url = "jdbc:trino://" + trino.getHost() + ":" + trino.getFirstMappedPort();
        return DriverManager.getConnection(url, props);
    }
}
