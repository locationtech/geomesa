/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.jdbc.QueryStats;
import io.trino.jdbc.StageStats;
import io.trino.jdbc.TrinoStatement;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.locationtech.geomesa.trino.datastore.TrinoFilterToSQL;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/**
 * Benchmarks the GeoMesa {@code spatial_iceberg} (SI) Trino connector — the spatial
 * file-pruning connector — across the filter suite in {@code benchmark_datasets.json}.
 * SI-only: no Accumulo path, no stock-iceberg comparison; each filter reports the
 * connector's own work.
 *
 * <p>For each dataset two kinds of rows are shown:
 * <ul>
 *   <li><b>count rows</b> (default): {@code SELECT count(*)} with the filter pushed down.
 *       Isolates connector scan time (file pruning + predicate pushdown) from JDBC
 *       streaming + WKB decode. Reports ms, count, rows-read, bytes-read, and pruning.</li>
 *   <li><b>[smoke] row</b> ({@code featuresOnly}): the full read path —
 *       SQL → JDBC → WKB decode → SimpleFeature — to exercise feature deserialization.
 *       Reports ms + count only (no per-operator scan probe on the GeoTools path).</li>
 * </ul>
 *
 * <p>Reported metrics per filter:
 * <ul>
 *   <li><b>ms</b>     — mean wall time across the measured runs.</li>
 *   <li><b>Count</b>  — row count matching the filter.</li>
 *   <li><b>Rows</b>   — physical input rows the scan actually read (post file-pruning).</li>
 *   <li><b>Bytes</b>  — physical input bytes the scan actually read.</li>
 *   <li><b>Pruned</b> — {@code 1 − files_read / total_files}, i.e. how much of the table
 *       the connector skipped. {@code files_read} is the leaf-stage split count from
 *       Trino JDBC stats; {@code total_files} is the table's {@code $files} count.</li>
 * </ul>
 *
 * Usage: mvn -pl geomesa-trino-benchmark exec:java [-Dexec.args="--runs 5 --warmup 2"]
 */
public class TrinoBenchmarkRunner {

    static final int DEFAULT_WARMUP = 1;
    static final int DEFAULT_RUNS   = 3;

    /** Catalog under benchmark — the GeoMesa spatial file-pruning connector. */
    static final String SI_CATALOG = "spatial_iceberg";

    /**
     * featuresOnly = true  → full feature-iteration via the DataStore read path (smoke test).
     * featuresOnly = false → SELECT COUNT(*) so the measurement reflects connector scan time
     *   (file pruning + predicate pushdown) without JDBC streaming + WKB decode of large results.
     */
    record FilterSpec(String label, String cql, boolean featuresOnly) {
        FilterSpec(String label, String cql) { this(label, cql, false); }
    }

    record DatasetConfig(String table, String label, List<FilterSpec> filters, String schema) {
        DatasetConfig(String table, String label, List<FilterSpec> filters) {
            this(table, label, filters, null);
        }
        /** Trino schema this table lives in, or {@code null} to use the run's
         *  {@code --schema} default (e.g. {@code rawob_*} live in {@code trino_test}). */
        String schemaOr(String defaultSchema) {
            return schema != null ? schema : defaultSchema;
        }
    }

    /**
     * Result of timed query runs plus optional scan stats from the last run.
     * {@code probe} is non-null when the run path captured JDBC QueryStats
     * (the {@link #runCount} path); {@code null} for the {@link #runFilter}
     * feature-iteration smoke path.
     */
    record BenchResult(long count, List<Double> times, ScanProbe probe) {
        BenchResult(long count, List<Double> times) { this(count, times, null); }
        double mean() {
            return times.stream().mapToDouble(x -> x).average().orElse(0);
        }
        double stdev() {
            if (times.size() < 2) return 0;
            double m = mean();
            return Math.sqrt(times.stream().mapToDouble(x -> (x - m) * (x - m)).sum() / (times.size() - 1));
        }
    }

    /**
     * Scan stats captured from a COUNT(*) probe with the same WHERE clause as the
     * timed run. {@code splits} (files read) come from Trino JDBC QueryStats;
     * rows/bytes come from the {@code /v1/query/<id>} REST endpoint's
     * {@code ScanFilterAndProjectOperator} entries.
     */
    record ScanProbe(int splits, long rowsRead, long bytesRead) {}

    // ── Dataset configs ───────────────────────────────────────────────────────
    //
    // Datasets and named geometry constants live in
    // src/main/resources/benchmark_datasets.json — externalized so adding or
    // tuning a dataset is a JSON edit, not a recompile. CQL strings reference
    // geometries via ${NAME} substitution; the loader inlines them at startup.

    static final List<DatasetConfig> DATASET_CONFIGS =
        DatasetConfigLoader.load("benchmark_datasets.json");

    // ── Benchmark execution ───────────────────────────────────────────────────

    static BenchResult runFilter(DataStore ds, String typeName, Filter filter,
                                  int warmup, int runs) throws IOException {
        SimpleFeatureSource fs = ds.getFeatureSource(typeName);
        Query q = new Query(typeName, filter);
        for (int i = 0; i < warmup; i++) iterateFeatures(fs, q);
        List<Double> times = new ArrayList<>(runs);
        int count = 0;
        for (int i = 0; i < runs; i++) {
            long t0 = System.nanoTime();
            count = iterateFeatures(fs, q);
            times.add((System.nanoTime() - t0) / 1_000_000.0);
        }
        return new BenchResult(count, times);
    }

    /**
     * Runs SELECT COUNT(*) directly via JDBC and reads the count as a long.
     * Bypasses the DataStore's {@code getCount(Query)} path because that returns
     * {@code int} per GeoTools' SimpleFeatureSource contract and returns -1 when
     * the actual count exceeds {@link Integer#MAX_VALUE}, clamping multi-billion-row
     * counts to zero in the benchmark output.
     *
     * <p>The last measured run additionally captures Trino's QueryStats and fetches
     * per-operator scan stats via the REST endpoint, so split count + rows/bytes read
     * are available on the returned {@link BenchResult#probe} — no separate probe
     * query needed.
     *
     * @param where pre-translated WHERE clause body (no leading "WHERE"), or
     *              {@code null} / empty for an unfiltered count.
     */
    static BenchResult runCount(String host, int port, String catalog, String schema,
                                 String table, String where, int warmup, int runs) {
        String sql = (where == null || where.isBlank())
            ? String.format("SELECT count(*) FROM \"%s\".\"%s\".\"%s\"",
                            catalog, schema, table)
            : String.format("SELECT count(*) FROM \"%s\".\"%s\".\"%s\" WHERE %s",
                            catalog, schema, table, where);
        String url = String.format("jdbc:trino://%s:%d/%s/%s", host, port, catalog, schema);
        Properties props = new Properties();
        props.setProperty("user", "benchmark");

        // Warmup runs: execute without capturing stats.
        for (int i = 0; i < warmup; i++) runOneCount(host, port, url, props, sql, false);

        // Measured runs: time each; capture stats only on the last run since the
        // query is deterministic and we only need one snapshot.
        List<Double> times = new ArrayList<>(runs);
        long count = 0;
        ScanProbe probe = null;
        for (int i = 0; i < runs; i++) {
            boolean capture = (i == runs - 1);
            long t0 = System.nanoTime();
            CountAndProbe r = runOneCount(host, port, url, props, sql, capture);
            count = r.count;
            if (capture) probe = r.probe;
            times.add((System.nanoTime() - t0) / 1_000_000.0);
        }
        return new BenchResult(count, times, probe);
    }

    private record CountAndProbe(long count, ScanProbe probe) {}

    /**
     * Executes one SELECT COUNT(*) and returns the count plus (optionally) the
     * scan stats. When {@code captureStats} is true, attaches a JDBC progress
     * monitor to grab the final {@link QueryStats}, then calls the REST endpoint
     * to read {@code physicalInputPositions} / {@code physicalInputDataSize}.
     */
    private static CountAndProbe runOneCount(String host, int port, String url,
                                              Properties props, String sql,
                                              boolean captureStats) {
        QueryStats[] last = new QueryStats[1];
        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt  = conn.createStatement()) {
            if (captureStats) {
                stmt.unwrap(TrinoStatement.class).setProgressMonitor(qs -> last[0] = qs);
            }
            long n;
            try (ResultSet rs = stmt.executeQuery(sql)) {
                n = rs.next() ? rs.getLong(1) : 0L;
                while (rs.next()) {} // drain so the query reaches FINISHED before close
            }
            ScanProbe probe = null;
            if (captureStats && last[0] != null) {
                int splits = last[0].getRootStage()
                    .map(TrinoBenchmarkRunner::deepestStageSplits)
                    .orElse(0);
                String queryId = last[0].getQueryId();
                long[] scan = queryId != null
                    ? fetchScanStats(host, port, queryId)
                    : new long[]{0L, 0L};
                probe = new ScanProbe(splits, scan[0], scan[1]);
            }
            return new CountAndProbe(n, probe);
        } catch (Exception e) {
            throw new RuntimeException("count query failed: " + e.getMessage(), e);
        }
    }

    /** Translates a GeoTools Filter to a Trino WHERE-clause body via {@link TrinoFilterToSQL}.
     *  Returns {@code null} for {@link Filter#INCLUDE} (caller treats as no-WHERE).
     *  The feature type drives the point-vs-non-point fast path in {@code visit(Intersects)},
     *  so the count-mode benchmark emits the SAME SQL the DataStore read path would — pass
     *  the SI table's schema so the measurement reflects production behavior. */
    static String filterToWhere(Filter filter, SimpleFeatureType featureType) {
        if (filter == Filter.INCLUDE) return null;
        try {
            TrinoFilterToSQL toSql = new TrinoFilterToSQL();
            if (featureType != null) toSql.setFeatureType(featureType);
            return toSql.encodeToString(filter);
        } catch (Exception e) {
            throw new RuntimeException("filter translation: " + e.getMessage(), e);
        }
    }

    static int iterateFeatures(SimpleFeatureSource fs, Query q) throws IOException {
        int n = 0;
        SimpleFeatureCollection fc = fs.getFeatures(q);
        try (SimpleFeatureIterator iter = fc.features()) {
            while (iter.hasNext()) { iter.next(); n++; }
        }
        return n;
    }

    // ── Table metadata ────────────────────────────────────────────────────────

    /** Reads the total file count from the table's {@code $files} metadata table
     *  (via the SI catalog — metadata-only, does not launch a scan). Used as the
     *  denominator for the pruning rate displayed alongside each filter row.
     *  Returns -1 if the metadata table is unavailable (pruning then shows "—"). */
    static int totalSplits(String host, int port, String schema, String table) {
        String sql = String.format(
            "SELECT count(*) FROM \"%s\".\"%s\".\"%s$files\"", SI_CATALOG, schema, table);
        String url = String.format("jdbc:trino://%s:%d/%s/%s", host, port, SI_CATALOG, schema);
        Properties props = new Properties();
        props.setProperty("user", "benchmark");
        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (Exception ex) {
            System.err.println("  [warn] totalSplits($files) failed: " + ex.getMessage());
            return -1;
        }
    }

    private static final ObjectMapper SCAN_STATS_MAPPER = new ObjectMapper();

    /** Pulls operator-level scan stats from Trino's REST endpoint. Returns
     *  {@code [rowsRead, bytesRead]} summed across every ScanFilterAndProjectOperator
     *  entry in {@code queryStats.operatorSummaries}. Important: only the top-level
     *  array — stage-level operatorSummaries re-emit the same operators and would
     *  cause double-counting. */
    static long[] fetchScanStats(String host, int port, String queryId) {
        try {
            URI uri = URI.create("http://" + host + ":" + port + "/v1/query/"
                                  + URLEncoder.encode(queryId, StandardCharsets.UTF_8));
            HttpRequest req = HttpRequest.newBuilder(uri)
                .header("X-Trino-User", "trino")
                .timeout(java.time.Duration.ofSeconds(10))
                .build();
            HttpResponse<String> resp = HttpClient.newHttpClient()
                .send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return new long[]{0L, 0L};
            return parseScanStats(resp.body());
        } catch (Exception ex) {
            System.err.println("  [warn] fetchScanStats failed: " + ex.getMessage());
            return new long[]{0L, 0L};
        }
    }

    /** Sums physicalInputPositions and physicalInputDataSize across every
     *  ScanFilterAndProjectOperator entry in the {@code queryStats.operatorSummaries}
     *  array. Visible for tests. */
    static long[] parseScanStats(String json) throws Exception {
        JsonNode root = SCAN_STATS_MAPPER.readTree(json);
        JsonNode ops  = root.path("queryStats").path("operatorSummaries");
        long rows = 0L, bytes = 0L;
        if (ops.isArray()) {
            for (JsonNode op : ops) {
                if (!"ScanFilterAndProjectOperator".equals(op.path("operatorType").asText())) continue;
                rows  += op.path("physicalInputPositions").asLong(0);
                bytes += parseBytes(op.path("physicalInputDataSize").asText("0B"));
            }
        }
        return new long[]{rows, bytes};
    }

    /** Parses Trino-formatted data sizes, e.g. {@code "1.59GB"}, {@code "131MB"},
     *  {@code "414kB"}, {@code "0B"}. */
    static long parseBytes(String s) {
        if (s == null || s.isEmpty()) return 0L;
        String[] suffixes = {"GB", "MB", "kB", "B"};
        long[] mults = {1L << 30, 1L << 20, 1L << 10, 1L};
        for (int i = 0; i < suffixes.length; i++) {
            if (s.endsWith(suffixes[i])) {
                try {
                    double v = Double.parseDouble(s.substring(0, s.length() - suffixes[i].length()));
                    return (long) (v * mults[i]);
                } catch (NumberFormatException ignored) { return 0L; }
            }
        }
        try { return Long.parseLong(s); } catch (NumberFormatException ignored) { return 0L; }
    }

    static String fmtBytes(long n) {
        if (n >= 1L << 30) return String.format(Locale.ROOT, "%.1f GB", n / (double)(1L << 30));
        if (n >= 1L << 20) return String.format(Locale.ROOT, "%.1f MB", n / (double)(1L << 20));
        if (n >= 1L << 10) return String.format(Locale.ROOT, "%.1f kB", n / (double)(1L << 10));
        return n + " B";
    }

    /** Pruning rate for an SI scan: {@code 1 − files_read / total_files}, formatted
     *  as {@code "<pct>% (<read>/<total>)"}. {@code "—"} when not measurable
     *  (no probe, unfiltered, or {@code $files} unavailable). */
    static String fmtPruned(ScanProbe si, int totalSplits) {
        if (si == null || totalSplits <= 0) return "—";
        double pr = 100.0 * (1.0 - (double) si.splits() / totalSplits);
        return String.format(Locale.ROOT, "%.0f%% (%d/%d)", pr, si.splits(), totalSplits);
    }

    static int deepestStageSplits(StageStats stage) {
        if (stage.getSubStages().isEmpty()) return stage.getTotalSplits();
        int max = 0;
        for (StageStats sub : stage.getSubStages()) {
            max = Math.max(max, deepestStageSplits(sub));
        }
        return max;
    }

    // ── Formatting ────────────────────────────────────────────────────────────

    /** Width of the horizontal separator rules; matches the printf column layout. */
    private static final int SEP_WIDTH = 110;

    static void sep(char c) { System.out.println(String.valueOf(c).repeat(SEP_WIDTH)); }
    static void sep()       { sep('─'); }

    static DataStore connect(String host, int port, String catalog, String schema,
                              String icebergRestUrl) throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("host",           host);
        params.put("port",           port);
        params.put("catalog",        catalog);
        params.put("schema",         schema);
        params.put("icebergRestUrl", icebergRestUrl);
        DataStore ds = DataStoreFinder.getDataStore(params);
        if (ds == null) throw new IOException(
            "DataStoreFinder returned null — TrinoDataStoreFactory not on classpath");
        return ds;
    }

    // ── Main ──────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        String host           = "localhost";
        int    port           = 8080;
        int    warmup         = DEFAULT_WARMUP;
        int    runs           = DEFAULT_RUNS;
        String icebergRestUrl = "http://localhost:8181";
        String schema         = "spatial";
        Set<String> datasetFilter = Set.of();  // empty = run all configured datasets

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--host"         -> host           = args[++i];
                case "--port"         -> port           = Integer.parseInt(args[++i]);
                case "--warmup"       -> warmup         = Integer.parseInt(args[++i]);
                case "--runs"         -> runs           = Integer.parseInt(args[++i]);
                case "--iceberg-rest" -> icebergRestUrl = args[++i];
                case "--schema"       -> schema         = args[++i];
                case "--datasets"     -> datasetFilter  =
                    new HashSet<>(Arrays.asList(args[++i].split(",")));
            }
        }

        // Apply --datasets filter (matches DatasetConfig.table()). Empty filter = all.
        final Set<String> activeFilter = datasetFilter;
        List<DatasetConfig> activeConfigs = activeFilter.isEmpty()
            ? DATASET_CONFIGS
            : DATASET_CONFIGS.stream().filter(c -> activeFilter.contains(c.table())).toList();
        if (!activeFilter.isEmpty() && activeConfigs.isEmpty()) {
            System.err.println("--datasets " + activeFilter + " matched no configured datasets. "
                + "Known: " + DATASET_CONFIGS.stream().map(DatasetConfig::table).toList());
            System.exit(2);
        }

        // Lazy DataStore cache: one SI entry per schema.
        Map<String, DataStore> dataStores = new HashMap<>();
        final String finalHost = host;
        final int    finalPort = port;
        final String finalIcebergRestUrl = icebergRestUrl;
        java.util.function.Function<String, DataStore> getSiDs = dataSchema ->
            dataStores.computeIfAbsent(dataSchema, k -> {
                try {
                    return connect(finalHost, finalPort, SI_CATALOG, k, finalIcebergRestUrl);
                } catch (Exception e) {
                    System.err.printf("Cannot connect to %s.%s at %s:%d: %s%n",
                        SI_CATALOG, k, finalHost, finalPort, e.getMessage());
                    return null;
                }
            });

        // Validate connectivity by opening the FIRST active dataset's schema.
        DataStore siProbe = getSiDs.apply(activeConfigs.get(0).schemaOr(schema));
        if (siProbe == null) {
            System.err.println("Cannot connect to Trino at " + host + ":" + port
                + " — is the stack running?");
            System.exit(1);
            return;
        }

        sep('═');
        System.out.println("  GeoMesa Trino DataStore Benchmark — spatial_iceberg (SI) connector");
        System.out.println("  Measure: SELECT COUNT(*) by default — isolates connector scan time");
        System.out.println("           (file pruning + predicate pushdown) from JDBC streaming +");
        System.out.println("           WKB-decode cost on large result sets. The trailing [smoke]");
        System.out.println("           row exercises the full read path on a tightly-selective filter.");
        System.out.printf( "  Config : %d warmup + %d measured runs%n", warmup, runs);
        sep('═');

        for (DatasetConfig cfg : activeConfigs) {
            String dataSchema = cfg.schemaOr(schema);
            DataStore siDs = getSiDs.apply(dataSchema);
            if (siDs == null) continue;

            Set<String> tables = new HashSet<>(Arrays.asList(siDs.getTypeNames()));
            if (!tables.contains(cfg.table())) continue;

            // Feature type for the SI table — drives the point fast path in
            // filterToWhere so count-mode SQL matches the production read path.
            SimpleFeatureType siSchema = siDs.getSchema(cfg.table());

            System.out.println();
            sep('═');
            System.out.println("  " + cfg.label() + " [" + dataSchema + "." + cfg.table() + "]");
            sep('═');

            System.out.printf("  %-46s  %8s  %11s  %12s  %11s  %16s%n",
                "Filter", "ms", "Count", "Rows", "Bytes", "Pruned (rd/tot)");
            sep();

            // Total file count once per table — denominator for the pruning %.
            int tableTotalSplits = totalSplits(finalHost, finalPort, dataSchema, cfg.table());

            boolean smokeSeparatorPrinted = false;
            List<Double> prunedPcts = new ArrayList<>();

            for (FilterSpec fs : cfg.filters()) {
                // Separator before the first feature-iteration smoke row, so the
                // count-mode (scan-time) rows are visually distinct from the
                // feature-mode (full read path) row.
                if (fs.featuresOnly() && !smokeSeparatorPrinted) {
                    sep();
                    smokeSeparatorPrinted = true;
                }

                Filter filter;
                try {
                    filter = "INCLUDE".equals(fs.cql()) ? Filter.INCLUDE : CQL.toFilter(fs.cql());
                } catch (CQLException e) {
                    System.err.printf("  [SKIP] %s — CQL parse error: %s%n", fs.label(), e.getMessage());
                    continue;
                }

                BenchResult result;
                try {
                    if (fs.featuresOnly()) {
                        // Full read path: SQL → JDBC → WKB decode → SimpleFeature.
                        // No per-operator scan probe on the GeoTools path.
                        result = runFilter(siDs, cfg.table(), filter, warmup, runs);
                    } else {
                        String where = filterToWhere(filter, siSchema);
                        result = runCount(finalHost, finalPort, SI_CATALOG,
                                          dataSchema, cfg.table(), where, warmup, runs);
                    }
                } catch (Exception e) {
                    System.err.printf("  [SKIP] %s — %s%n", fs.label(), e.getMessage());
                    continue;
                }

                boolean unfiltered = "INCLUDE".equals(fs.cql());
                ScanProbe probe = result.probe();
                String rowsStr  = probe != null ? String.format(Locale.ROOT, "%,d", probe.rowsRead())  : "—";
                String bytesStr = probe != null ? fmtBytes(probe.bytesRead())                          : "—";
                String prunedStr = unfiltered ? "—" : fmtPruned(probe, tableTotalSplits);

                System.out.printf(Locale.ROOT,
                    "  %-46s  %8.0f  %,11d  %12s  %11s  %16s%n",
                    fs.label(), result.mean(), result.count(), rowsStr, bytesStr, prunedStr);

                // Average pruning across the filtered count-mode rows (skip the
                // unfiltered baseline and the smoke feature row).
                if (!fs.featuresOnly() && !unfiltered && probe != null && tableTotalSplits > 0) {
                    prunedPcts.add(100.0 * (1.0 - (double) probe.splits() / tableTotalSplits));
                }
            }

            if (!prunedPcts.isEmpty()) {
                sep();
                double avgPruned = prunedPcts.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                System.out.printf(Locale.ROOT, "  %-46s  %8s  %11s  %12s  %11s  %15.0f%%%n",
                    "avg pruning across filtered count queries", "", "", "", "", avgPruned);
            }
        }

        System.out.println();
        sep('═');
        System.out.println("  SI     = spatial_iceberg (Z2/XZ2 spatial file-pruning connector)");
        System.out.println("  ms     = mean wall time over the measured runs");
        System.out.println("  Rows   = physical input rows the scan read (post file-pruning); — on the [smoke] row");
        System.out.println("  Bytes  = physical input bytes the scan read");
        System.out.println("  Pruned = 1 − files_read / total_files (rd/tot = files read / table $files count)");
        System.out.println("           — when unfiltered, on the [smoke] row, or if $files is unavailable.");
        sep('═');

        for (DataStore ds : dataStores.values()) {
            if (ds != null) ds.dispose();
        }
        System.exit(0);
    }
}
