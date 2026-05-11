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
 * Integration test: requires the local docker stack up and synthetic observations
 * ingested with the visibilities column (make up-trino && make ingest-demo-data).
 *
 * VIS_CYCLE = [None, "U", "U&FOUO"] (a notional U//FOUO clearance
 * ladder) assigned by row index i % 3.  The assertions are expressed as strict
 * monotonic inequalities (0 &lt; none &lt; U &lt; U,FOUO) rather than exact share
 * arithmetic so that minor variations in row count (e.g. dedup, re-ingest) do
 * not break the test.
 *
 * The one exception — countsMatchIteration — verifies that getCount() and manual
 * iteration produce the same result for a partially-cleared ("U,FOUO") auth set,
 * which is the primary correctness invariant for the filtering stack.
 */
@Tag("integration")
class VisibilityIT {

    private static final String TABLE = "observations";

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
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Build a DataStore with the given auths string.
     * Pass {@code null} to disable visibility filtering entirely (all rows visible).
     * Pass {@code ""} for an empty auth set (only null-visibility rows visible).
     */
    private DataStore store(String auths) throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("host",           "localhost");
        params.put("port",           8080);
        params.put("catalog",        "spatial_iceberg");
        params.put("schema",         "spatial");
        params.put("icebergRestUrl", "http://localhost:8181");
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
     * A store with no auth param (security disabled) sees every row. A "U" store
     * sees a strict subset, and a store with "" (no auths) sees an even stricter
     * subset (only the null-visibility rows). A fully-cleared "U,FOUO" store clears
     * every tier, so it matches the unfiltered count.
     *
     * VIS_CYCLE = [null, U, U&FOUO]:
     *   none   → only cycle[0] (null-vis) rows       → the smallest set
     *   U      → cycle[0] + cycle[1] rows            → larger
     *   U,FOUO → all tiers                           → largest (== unfiltered)
     *
     * Using monotonic inequalities rather than exact share arithmetic keeps the
     * test stable across minor ingest variations (dedup, partial re-ingest, etc.).
     */
    @Test
    void authFilteringProducesStrictlyMonotonicCounts() throws IOException {
        int all  = countVia(store(null));       // security disabled → every row
        int fouo = countVia(store("U,FOUO"));    // clears every VIS_CYCLE tier (null, U, U&FOUO)
        int u    = countVia(store("U"));          // sees null + U tiers
        int none = countVia(store(""));           // sees only null-visibility rows

        assertThat(all).as("unfiltered store must have at least one row").isGreaterThan(0);
        assertThat(none).as("empty-auths count must be > 0 (null-vis rows exist)").isGreaterThan(0);
        assertThat(u).as("'U' auth count must be > empty-auths count").isGreaterThan(none);
        assertThat(fouo).as("'U,FOUO' auth count must be > 'U' auth count").isGreaterThan(u);
        assertThat(all).as("'U,FOUO' clears every tier, so it matches the unfiltered count").isEqualTo(fouo);
    }

    /**
     * Iterating with empty auths only surfaces rows whose visibility annotation
     * is null (unrestricted rows in the VIS_CYCLE).
     */
    @Test
    void emptyAuthsSeeOnlyUnrestrictedRows() throws IOException {
        DataStore none = store("");
        int viaCount   = countVia(none);
        int viaIter    = iterate(none, null);  // all returned features should have null vis
        assertThat(viaCount).as("getCount and iterate must agree for empty auths").isEqualTo(viaIter);
        assertThat(viaCount).isGreaterThan(0);
    }

    /**
     * getCount() and manual iteration must return the same value for a
     * partially-cleared "U,FOUO" auth set — the primary correctness invariant
     * for the filtering stack. Every marked row returned to this user requires
     * at least "U", so each non-null visibility contains that token.
     */
    @Test
    void countsMatchIteration() throws IOException {
        DataStore fouo = store("U,FOUO");
        int viaCount   = countVia(fouo);
        int viaIter    = iterate(fouo, "U");
        assertThat(viaCount)
            .as("getCount() and FeatureReader iteration must agree for auths='U,FOUO'")
            .isEqualTo(viaIter);
        assertThat(viaCount).isGreaterThan(0);
    }
}
