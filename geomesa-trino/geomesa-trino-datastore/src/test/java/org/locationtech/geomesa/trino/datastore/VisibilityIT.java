/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.security.SecurityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: requires a running Trino at localhost:8080 with the plugin
 * loaded and the demo synthetic observations ingested with the __vis__ column.
 *
 * <p>The ingested data must carry a three-tier visibility ladder assigned by row
 * index i % 3: {@code [null, P, P&Q]}, where P is the partial-clearance token and
 * P&amp;Q the full set. The non-null tokens are DETECTED from the table's distinct
 * {@code __vis__} values (via the plain catalog), so the test runs flag-free
 * against any deployment's ladder; explicit overrides remain available:
 * <pre>
 *   -Dgeomesa.it.auths.partial=&lt;P&gt;      (default "basic")
 *   -Dgeomesa.it.auths.full=&lt;P,Q,...&gt;   (default "basic,privileged")
 * </pre>
 * See {@link TestFixtures#visibilityLadder}.
 *
 * <p><strong>The tier-0 (NULL) rows are hidden from every caller</strong> — a NULL
 * or empty {@code __vis__} carries no real visibility expression, so the shared
 * {@code is_visible} UDF treats it as an anomaly and admits it for no one, not even
 * a full-auth identity. So the observable ladder is: empty auths see <em>nothing</em>
 * (0 rows), the partial token clears tier 1, and the full set clears tiers 1+2.
 *
 * <p>The assertions are expressed as (near-)monotonic inequalities
 * (0 == none &lt; partial &lt; full) rather than exact share arithmetic so that minor
 * variations in row count (e.g. dedup, re-ingest) do not break the test.
 *
 * The one exception — countsMatchIteration — verifies that getCount() and manual
 * iteration produce the same result for a partially-cleared (full-auth) set,
 * which is the primary correctness invariant for the filtering stack.
 */
@Tag("integration")
class VisibilityIT {

    private static final String TABLE = "observations";

    /** Auth token granting the middle visibility tier; resolved in
     *  {@link #requiresTrino} (see class javadoc). */
    private static String PARTIAL_AUTH;

    /** Comma-separated auth set clearing every tier; resolved in
     *  {@link #requiresTrino} (see class javadoc). */
    private static String FULL_AUTHS;

    private final List<DataStore> stores = new ArrayList<>();

    @AfterEach
    void disposeStores() {
        stores.forEach(DataStore::dispose);
        stores.clear();
    }

    @BeforeAll
    static void requiresTrino() {
        try (var socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress("localhost", 8080), 2000);
        } catch (Exception e) {
            Assumptions.assumeTrue(false,
                "Trino not reachable at localhost:8080 — skipping visibility integration tests");
        }
        Assumptions.assumeTrue(TestFixtures.ensureTable(TABLE),
            "spatial." + TABLE + " not ingested and not provisionable — skipping (see class javadoc)");
        // Resolve AFTER ensureTable: freshly-provisioned data is written from the
        // same properties/defaults, so detection and data always agree.
        String[] ladder = TestFixtures.visibilityLadder(TABLE);
        PARTIAL_AUTH = ladder[0];
        FULL_AUTHS = ladder[1];
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Build a DataStore with the given auths string.
     * Pass {@code null} for no security params (the store adds no is_visible conjunct; the
     * connector's server-side row filter still applies for the connecting identity).
     * Pass {@code ""} for an empty auth set — no visibility expression is satisfiable and
     * NULL/empty rows are hidden anomalies, so the caller sees no rows at all.
     */
    private DataStore store(String auths) throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("trino.host",    "localhost");
        params.put("trino.port",    8080);
        params.put("trino.catalog", "spatial_iceberg");
        params.put("trino.schema",  "spatial");
        if (auths != null) {
            params.put("geomesa.security.auths", auths);
        }
        DataStore ds = DataStoreFinder.getDataStore(params);
        assertThat(ds).as("DataStoreFinder must locate TrinoDataStoreFactory").isNotNull();
        stores.add(ds);
        return ds;
    }

    /** Count via {@code FeatureSource.getCount(Query.ALL)}. */
    private static int countVia(DataStore ds) throws IOException {
        return ds.getFeatureSource(TABLE).getCount(Query.ALL);
    }

    /**
     * Iterate all features and return the count.
     * If {@code expectAuthContained} is non-null, assert that every feature
     * whose visibility is non-null contains that token.
     */
    private static int iterate(DataStore ds, String expectAuthContained) throws IOException {
        int n = 0;
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                 ds.getFeatureReader(new Query(TABLE), Transaction.AUTO_COMMIT)) {
            while (reader.hasNext()) {
                SimpleFeature f = reader.next();
                String vis = SecurityUtils.getVisibility(f);
                if (vis != null && expectAuthContained != null) {
                    assertThat(vis).contains(expectAuthContained);
                }
                n++;
            }
        }
        return n;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /**
     * Auths are fail-closed end-to-end: an UNCONFIGURED store (no geomesa.security
     * params) behaves exactly like a store with explicit empty auths — both see
     * NO rows. An empty auth set satisfies no visibility expression, and the tier-0
     * NULL rows are anomalies hidden from everyone (see class javadoc). Granting
     * auths then widens the result set tier by tier.
     *
     * Visibility ladder = [null, partial, partial&rest] (see class javadoc):
     *   unconfigured / "" → 0 rows (no expression satisfiable, NULL rows hidden)
     *   partial auth        → tier 1 rows only                → larger
     *   full auths          → tier 1 + tier 2 rows            → largest
     *
     * Using inequalities rather than exact share arithmetic keeps the test stable
     * across minor ingest variations (dedup, partial re-ingest, etc.).
     */
    @Test
    void authFilteringProducesStrictlyMonotonicCounts() throws IOException {
        int unconfigured = countVia(store(null)); // no security params → fail-closed
        int full    = countVia(store(FULL_AUTHS));   // clears tiers 1 and 2
        int partial = countVia(store(PARTIAL_AUTH)); // clears tier 1 only
        int none    = countVia(store(""));           // empty auths → no rows

        assertThat(none).as("empty-auths count must be 0 (no expression satisfiable; NULL rows hidden)").isZero();
        assertThat(partial)
            .as("'%s' auth count must be > empty-auths count", PARTIAL_AUTH).isGreaterThan(none);
        assertThat(full)
            .as("'%s' auth count must be > '%s' auth count", FULL_AUTHS, PARTIAL_AUTH)
            .isGreaterThan(partial);
        assertThat(unconfigured)
            .as("an unconfigured store is fail-closed: it matches the explicit empty-auths count")
            .isEqualTo(none);
    }

    /**
     * With empty auths the caller sees no rows at all: no visibility expression is
     * satisfiable, and the tier-0 NULL rows are anomalies hidden from everyone. Both
     * the pushed-down count and a full iteration return zero, and they agree.
     */
    @Test
    void emptyAuthsSeeNoRows() throws IOException {
        DataStore none = store("");
        int viaCount   = countVia(none);
        int viaIter    = iterate(none, null);
        assertThat(viaCount).as("getCount and iterate must agree for empty auths").isEqualTo(viaIter);
        assertThat(viaCount)
            .as("empty auths see no rows (NULL rows hidden, no expression satisfiable)")
            .isZero();
    }

    /**
     * getCount() and manual iteration must return the same value for the
     * full auth set — the primary correctness invariant for the filtering
     * stack. Every marked row in the ladder requires at least the partial
     * token, so each non-null visibility contains it.
     */
    @Test
    void countsMatchIteration() throws IOException {
        DataStore full = store(FULL_AUTHS);
        int viaCount   = countVia(full);
        int viaIter    = iterate(full, PARTIAL_AUTH);
        assertThat(viaCount)
            .as("getCount() and FeatureReader iteration must agree for auths='%s'", FULL_AUTHS)
            .isEqualTo(viaIter);
        assertThat(viaCount).isGreaterThan(0);
    }
}
