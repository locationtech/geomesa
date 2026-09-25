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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Specifies, end to end on a real Trino, that an explicit {@code is_visible("__vis__", '<auths>')}
 * query predicate narrows visibility-column file pruning to the auths the query asks for, on top
 * of the auths the file resolver maps to the connecting user.
 *
 * <p>Five files of 100 rows, one visibility value each: {@code admin}, {@code ops},
 * {@code finance}, {@code ops&finance} and all-NULL. Every mapped user is crossed with a set of
 * predicates. Each cell asserts:
 * <ul>
 *   <li><b>rows returned</b> — rows passing both the user's auths and the predicate (correctness;
 *       passes today);</li>
 *   <li><b>rows read</b> ({@code physicalInputPositions}, which file pruning lowers and row
 *       filtering does not) — only the files admissible to <em>both</em> the user's auths and every
 *       top-level {@code is_visible} conjunct. Cells where the predicate is narrower than the user's
 *       auths fail until the predicate is honored for pruning.</li>
 * </ul>
 * Guard cells (a broader predicate, and {@code is_visible(...) OR id = 1}) must not narrow or widen
 * pruning; they pass today and must keep passing.
 *
 * <p>A PASS/FAIL matrix of every cell is written to {@code target/visibility-pruning-matrix.md}.
 *
 * <p>The catalog under test is this repo's {@code spatial_iceberg} connector, loaded into the Trino
 * container from the plugin zip named by {@code trino.plugin.path.*}; {@code setUp} fails every cell
 * if it is not. Build the plugin in the same reactor so that zip is the one built from your working
 * copy rather than a stale snapshot in the local repository:
 * <pre>
 *   mvn verify failsafe:integration-test failsafe:verify \
 *     -pl geomesa-trino/geomesa-trino-plugin,geomesa-trino/geomesa-trino-datastore \
 *     -DskipITs=false -Dit.test=VisibilityPruningPredicateIT -Dtest=none \
 *     -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
@Tag("integration")
class VisibilityPruningPredicateIT {

    private static final String WRITE_CATALOG = "iceberg";
    private static final String SCHEMA = "vis";
    /** Catalog under test: must be served by this repo's spatial_iceberg connector. */
    private static final String READ_CATALOG = "spatial";
    private static final String READ_TABLE = READ_CATALOG + "." + SCHEMA + ".t";
    private static final String WRITE_TABLE = WRITE_CATALOG + "." + SCHEMA + ".t";
    private static final int ROWS_PER_FILE = 100;

    /** Visibility value of each data file; null is the unrestricted file. */
    private static final List<String> FILES = Arrays.asList("admin", "ops", "finance", "ops&finance", null);

    /** Mirrors docker/trino/pruning-predicate/geomesa-auths.properties. */
    private static final Map<String, Set<String>> USERS = new LinkedHashMap<>();
    static {
        USERS.put("allauths", Set.of("admin", "ops", "finance"));
        USERS.put("opsfin", Set.of("ops", "finance"));
        USERS.put("opsuser", Set.of("ops"));
        USERS.put("finuser", Set.of("finance"));
        USERS.put("nobody", Set.of());
    }

    /**
     * A query predicate. {@code pruneAuths} is the auths pruning may narrow to (the intersection
     * of every top-level is_visible conjunct), or null when the predicate must not narrow pruning.
     * {@code rowAuths} are the auths each row is checked against, and {@code orIdEqualsOne} marks
     * the {@code ... OR id = 1} guard, which also returns id 1 from every readable file.
     */
    private record Predicate(String label, String where, Set<String> pruneAuths, Set<String> rowAuths,
                             boolean orIdEqualsOne) {
        @Override
        public String toString() {
            return label;
        }
    }

    private static Predicate conjunct(String auths) {
        return new Predicate("is_visible(__vis__, '" + auths + "')",
                "WHERE is_visible(\"__vis__\", '" + auths + "')", parse(auths), parse(auths), false);
    }

    private static final List<Predicate> PREDICATES = List.of(
            new Predicate("–", "", null, null, false),
            conjunct("ops"),
            conjunct("finance"),
            conjunct("ops,finance"),
            conjunct("admin,ops,finance"),
            conjunct(""),
            new Predicate("is_visible(__vis__, 'ops,finance') AND is_visible(__vis__, 'finance')",
                    "WHERE is_visible(\"__vis__\", 'ops,finance') AND is_visible(\"__vis__\", 'finance')",
                    Set.of("finance"), Set.of("finance"), false),
            new Predicate("is_visible(__vis__, 'ops') OR id = 1 (guard)",
                    "WHERE is_visible(\"__vis__\", 'ops') OR id = 1",
                    null, Set.of("ops"), true));

    private static final List<String> MATRIX = Collections.synchronizedList(new ArrayList<>());

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
                    .withFileSystemBind(resource("docker/trino/pruning-predicate/iceberg.properties"),
                            "/etc/trino/catalog/iceberg.properties", BindMode.READ_ONLY)
                    .withFileSystemBind(resource("docker/trino/pruning-predicate/spatial.properties"),
                            "/etc/trino/catalog/spatial.properties", BindMode.READ_ONLY)
                    .withFileSystemBind(resource("docker/trino/pruning-predicate/geomesa-auths.properties"),
                            "/etc/trino/geomesa-auths.properties", BindMode.READ_ONLY);

    private static String resource(String classpath) {
        return MountableFile.forClasspathResource(classpath).getResolvedPath();
    }

    /** Rows physically read from storage, and rows returned to the client. */
    private record Read(long physical, long returned) {}

    @BeforeAll
    static void setUp() throws Exception {
        s3.start();
        iceberg.start();
        trino.start();

        assertCatalogUnderTestIsOurPlugin();

        try (Connection conn = connect("admin");
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + WRITE_CATALOG + "." + SCHEMA);
            stmt.execute("CREATE TABLE " + WRITE_TABLE + " (id bigint, \"__vis__\" varchar)");
            // One INSERT per visibility value => one data file each, min == max on __vis__.
            for (String vis : FILES) {
                String value = vis == null ? "CAST(NULL AS varchar)" : "'" + vis + "'";
                stmt.execute("INSERT INTO " + WRITE_TABLE + " SELECT x, " + value
                        + " FROM UNNEST(sequence(1, " + ROWS_PER_FILE + ")) AS u(x)");
            }
        }
    }

    /**
     * Fails every cell up front unless the catalog under test is served by this repo's
     * {@code spatial_iceberg} connector (loaded from the plugin zip built from this checkout) and
     * its {@code is_visible} function is registered. Otherwise the rows-read numbers would describe
     * some other connector and prove nothing about this one.
     */
    private static void assertCatalogUnderTestIsOurPlugin() throws Exception {
        String zip = System.getProperty("trino.plugin.path.2.12", System.getProperty("trino.plugin.path.2.13"));
        System.out.println("Catalog '" + READ_CATALOG + "' plugin zip: " + zip);
        try (Connection conn = connect("admin");
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT connector_name FROM system.metadata.catalogs"
                    + " WHERE catalog_name = '" + READ_CATALOG + "'")) {
                assertThat(rs.next()).as("catalog '" + READ_CATALOG + "' is registered").isTrue();
                assertThat(rs.getString(1)).as("connector serving catalog '" + READ_CATALOG + "'")
                        .isEqualTo("spatial_iceberg");
            }
            try (ResultSet rs = stmt.executeQuery("SHOW FUNCTIONS LIKE 'is_visible'")) {
                assertThat(rs.next()).as("is_visible is registered by the geomesa plugin").isTrue();
            }
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        List<String> rows = new ArrayList<>(MATRIX);
        String table = "| User | Mapped auths | Query predicate | Rows returned (expected) | "
                + "Rows read | Rows read expected | Result |\n|---|---|---|---|---|---|---|\n"
                + String.join("\n", rows) + "\n";
        Files.writeString(Path.of("target", "visibility-pruning-matrix.md"), table);
        System.out.println(table);

        trino.stop();
        iceberg.stop();
        s3.stop();
    }

    static Stream<Arguments> cases() {
        return USERS.keySet().stream().flatMap(user -> PREDICATES.stream()
                .map(p -> Arguments.of(user, p)));
    }

    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("cases")
    void pruningHonorsIsVisiblePredicate(String user, Predicate predicate) throws Exception {
        Set<String> userAuths = USERS.get(user);
        long expectedRead = 0;
        long expectedReturned = 0;
        for (String vis : FILES) {
            if (!visible(vis, userAuths)) {
                continue; // the row filter hides the whole file, and pruning by user auths skips it
            }
            boolean rowsPass = predicate.rowAuths() == null || visible(vis, predicate.rowAuths());
            boolean readable = predicate.pruneAuths() == null || visible(vis, predicate.pruneAuths());
            if (readable) {
                expectedRead += ROWS_PER_FILE;
            }
            if (rowsPass) {
                expectedReturned += ROWS_PER_FILE;
            } else if (predicate.orIdEqualsOne()) {
                expectedReturned += 1;
            }
        }

        Read r = read(user, predicate.where());

        boolean pass = r.returned() == expectedReturned && r.physical() == expectedRead;
        MATRIX.add("| " + user
                + " | " + (userAuths.isEmpty() ? "–" : userAuths.stream().sorted().collect(Collectors.joining(",")))
                + " | `" + predicate.label() + "`"
                + " | " + r.returned() + (r.returned() == expectedReturned ? "" : " (expected " + expectedReturned + ")")
                + " | " + r.physical()
                + " | " + expectedRead
                + " | " + (pass ? "PASS" : "**FAIL**") + " |");

        assertThat(r.returned()).as("rows returned").isEqualTo(expectedReturned);
        assertThat(r.physical())
                .as("rows physically read: only files admissible to both the user's auths and the predicate")
                .isEqualTo(expectedRead);
    }

    /** Minimal evaluator for this fixture's values: NULL is unrestricted, {@code a&b} needs both. */
    private static boolean visible(String vis, Set<String> auths) {
        if (vis == null || vis.isEmpty()) {
            return true;
        }
        return Arrays.stream(vis.split("&")).allMatch(auths::contains);
    }

    private static Set<String> parse(String csv) {
        return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
    }

    /** Runs {@code SELECT id FROM t <where>} as {@code user}, draining the result. */
    private Read read(String user, String where) throws Exception {
        String queryId;
        long returned = 0;
        try (Connection conn = connect(user);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM " + READ_TABLE + " " + where)) {
            while (rs.next()) {
                rs.getLong(1);
                returned++;
            }
            queryId = rs.unwrap(TrinoResultSet.class).getQueryId();
        }
        return new Read(physicalInputPositions(queryId), returned);
    }

    /** Reads {@code queryStats.physicalInputPositions} from the coordinator's REST query info. */
    private long physicalInputPositions(String queryId) throws Exception {
        URI uri = URI.create("http://" + trino.getHost() + ":" + trino.getFirstMappedPort()
                + "/v1/query/" + queryId);
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
