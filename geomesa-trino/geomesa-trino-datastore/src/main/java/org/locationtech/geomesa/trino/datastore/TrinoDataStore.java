/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.feature.type.Name;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory;
import org.locationtech.geomesa.security.AuthorizationsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Read-only GeoTools {@link ContentDataStore} backed by a Trino/Iceberg catalog, with Z2/XZ2
 * partition pruning and row-level visibility. The datastore is the trusted intermediary that
 * forwards each caller's authorizations to Trino per query as an extra credential.
 *
 * <p><strong>Why {@code ContentDataStore} and not {@code JDBCDataStore}?</strong> As of this version, the
 * Trino store is read-only to function primarily as a query engine and does not expose the write-based operations
 * provided by {@code JDBCDataStore}. {@code ContentDataStore} is the lighter, correct base for a query-only source,
 * it lets us push filter/projection/sort/count/bounds down as Trino SQL (see {@link TrinoFeatureSource}), and
 * offers per-request-authenticated connections per query and visibility/entitlement filtering. Because the
 * forwarded authorizations are fixed on a connection at creation, connections are pooled keyed by the normalized
 * auth set ({@link ConnectionPool}): requests with the same effective auths reuse connections (see {@link #connect(List)}).
 */
public class TrinoDataStore extends ContentDataStore {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoDataStore.class);
    private static final long TYPE_NAMES_TTL_MILLIS = TimeUnit.SECONDS.toMillis(60);

    /** Default Trino connection user — a dedicated service account. The datastore
     *  is the trusted intermediary: it forwards each caller's authorizations to
     *  Trino per query as an extra credential (see {@link #connect(List)}), so the
     *  connector's row filter enforces the real per-request auths rather than a
     *  blanket service-account grant. Override via the {@code user} connection param. */
    static final String DEFAULT_USER = "geomesa";

    /** Trino extra-credential name carrying the caller's authorization tokens.
     *  Must match {@code geomesa.security.auths-credential} on the catalog (whose
     *  default is also {@code auths}). */
    static final String AUTHS_CREDENTIAL = "auths";

    /** Trino extra-credential name carrying the shared secret that gates whether
     *  the connector honors the auths credential. Must match the catalog's
     *  {@code geomesa.security.secret-credential} (default {@code secret}). */
    static final String SECRET_CREDENTIAL = "secret";

    private final String host;
    private final int port;
    private final String catalog;
    private final String trinoSchema;
    private final AuthorizationsProvider authProvider;
    private final String user;
    /** Shared secret presented to the connector; null/empty when the catalog isn't secret-gated. */
    private final String secret;
    private final Closeable registry;

    private final Object typeNamesLock = new Object();
    private Map<String, String> cachedTypeNames;
    private long cachedTypeNamesExpiry = 0L;

    /** Connections keyed by normalized auth set — see {@link #connect(List)}. */
    private final ConnectionPool pool = new ConnectionPool(this::openConnection);

    TrinoDataStore(String host, int port, String catalog, String schema,
                   AuthorizationsProvider authProvider, String user, String secret, GeoMesaDataStoreFactory.MetricsConfig metrics) {
        this.host = host;
        this.port = port;
        this.catalog = catalog;
        this.trinoSchema = schema;
        this.authProvider = authProvider;
        this.user = user;
        this.secret = secret;
        this.registry = metrics == null ? null : metrics.register();
    }

    /** Null when no security params were supplied (filtering disabled). */
    AuthorizationsProvider authProvider() { return authProvider; }

    /** Metadata connection (no caller auths) — for table listing against
     *  information_schema, which the connector never row-filters. */
    Connection connect() throws SQLException {
        return connect(null);
    }

    /** Returns a Trino connection carrying the caller's authorizations as a Trino
     *  extra credential. The {@code spatial_iceberg} connector's
     *  ExtraCredentialAuthorizationResolver reads it to build the row-level
     *  visibility filter for THIS request. Because the credential is fixed at
     *  connection creation, connections are pooled keyed by the normalized
     *  (sorted, deduped) auth set: a request only ever gets a connection whose
     *  credential matches its own auths exactly. Close the connection to return
     *  it to the pool. When {@code auths} is null/empty no credential is sent
     *  (the caller then sees only unrestricted rows — fail-closed). */
    Connection connect(List<String> auths) throws SQLException {
        return pool.borrow(auths);
    }

    /** Opens a real connection for a normalized auth set (pool miss). */
    private Connection openConnection(List<String> normalizedAuths) throws SQLException {
        String url = String.format("jdbc:trino://%s:%d/%s/%s",
            host, port, catalog, trinoSchema);
        return DriverManager.getConnection(url, connectionProperties(user, normalizedAuths, secret));
    }

    /** Closes all pooled connections. */
    @Override
    public void dispose() {
        // noinspection EmptyTryBlock
        try (pool; registry) {
            // just use try for auto close
        } catch (IOException e) {
            LOG.error("Error disposing data store:", e);
        } finally {
            super.dispose();
        }
    }

    /** Builds the JDBC connection properties. Extracted so the extra-credential
     *  encoding is unit-testable without opening a real connection.
     *
     *  <p>The Trino JDBC {@code extraCredentials} property is {@code name:value}
     *  pairs delimited by SEMICOLONS (NOT commas), and each value must be printable
     *  ASCII with no spaces. The auth tokens are comma-joined and then base64url
     *  encoded (unpadded) into a single value — the base64url alphabet can never
     *  collide with the wire delimiters, so the transport is injection-proof by
     *  construction: {@code auths:<base64url(tok1,tok2)>;secret:<value>}. The
     *  server-side {@code ExtraCredentialAuthorizationResolver} decodes the same
     *  format.
     *
     *  <p>Tokens are still validated against {@link AuthTokens} before encoding:
     *  the SQL row-filter layer joins tokens with commas into the
     *  {@code is_visible} literal, so the no-delimiter token contract remains
     *  load-bearing there even though the wire itself no longer requires it. */
    static Properties connectionProperties(String user, List<String> auths, String secret) {
        AuthTokens.validate(auths);
        Properties props = new Properties();
        props.setProperty("user", user);
        StringBuilder creds = new StringBuilder();
        if (auths != null && !auths.isEmpty()) {
            String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(String.join(",", auths).getBytes(StandardCharsets.UTF_8));
            creds.append(AUTHS_CREDENTIAL).append(':').append(encoded);
        }
        if (secret != null && !secret.isEmpty()) {
            if (creds.length() > 0) creds.append(';');  // semicolon delimits pairs
            creds.append(SECRET_CREDENTIAL).append(':').append(secret);
        }
        if (creds.length() > 0) {
            props.setProperty("extraCredentials", creds.toString());
        }
        return props;
    }

    String catalog()     { return catalog; }
    String trinoSchema() { return trinoSchema; }

    /** Double-quote a SQL identifier (column/table/schema/catalog name), escaping any
     *  embedded quotes, so caller-influenced names (query property names, sort columns,
     *  discovered type names) can't break out of the identifier position. Same algorithm
     *  as the JDBC 4.3 {@link java.sql.Statement#enquoteIdentifier} default (alwaysQuote
     *  variant) — kept static here because SQL is composed, and unit-tested, before any
     *  {@code Statement} exists. */
    static String escapeQuotes(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Lists the available type (table) names, cached for the type-names TTL.
     *
     * @return the table names exposed by the configured Trino catalog/schema
     */
    @Override
    protected List<Name> createTypeNames() throws IOException {
        synchronized (typeNamesLock) {
            if (cachedTypeNames == null || System.currentTimeMillis() > cachedTypeNamesExpiry) {
                cachedTypeNames = queryTypeNames();
                cachedTypeNamesExpiry = System.currentTimeMillis() + TYPE_NAMES_TTL_MILLIS;
            }
            return new ArrayList<>(cachedTypeNames.keySet().stream().map(this::name).toList());
        }
    }

    // note: expects createTypeNames() to have already been called
    String getTableName(String typeName) {
        synchronized (typeNamesLock) {
            return cachedTypeNames.get(typeName);
        }
    }

    private Map<String, String> queryTypeNames() throws IOException {
        String sql = "SELECT table_name FROM " + escapeQuotes(catalog)
            + ".information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'";
        List<String> tableNames = new ArrayList<>();
        Map<String, String> names = new HashMap<>();
        try (Connection conn = connect()) {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, trinoSchema);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        tableNames.add(rs.getString("table_name"));
                    }
                }
            }

            for (String tableName: tableNames) {
                // check if the properties table exists - note that direct existence checks work,
                // but something like `where table_name like '%$properties'` will not return anything
                String checkPropertiesTable = String.format(
                        "SELECT 1 FROM %s.information_schema.tables WHERE table_schema = ? AND table_name = ?",
                        escapeQuotes(catalog)
                );
                boolean propertiesTableExists;
                try (PreparedStatement ps = conn.prepareStatement(checkPropertiesTable)) {
                    ps.setString(1, trinoSchema);
                    ps.setString(2, tableName + "$properties");
                    try (ResultSet rs = ps.executeQuery()) {
                        propertiesTableExists = rs.next();
                    }
                }

                if (!propertiesTableExists) {
                    LOGGER.info("Suppressing non-Iceberg table '" + tableName +
                            "' as it does not have a '$properties' metadata table");
                    continue;
                }

                String selectTableProperties = String.format(
                        "SELECT value FROM %s.%s.%s  WHERE key = 'geomesa.sft.name'",
                        escapeQuotes(catalog),
                        escapeQuotes(trinoSchema),
                        escapeQuotes(tableName + "$properties")
                );
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(selectTableProperties)) {
                    if (rs.next()) {
                        names.put(rs.getString(1), tableName);
                    } else {
                        LOGGER.info("Suppressing table '" + tableName +
                                "' as it does not have expected property 'geomesa.sft.name'");
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to list tables", e);
        }
        return Collections.unmodifiableMap(names);
    }

    /**
     * Creates the feature source backing a single table.
     *
     * @param entry the content entry identifying the table
     * @return a feature source for the entry's table
     */
    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) {
        return new TrinoFeatureSource(entry, this);
    }
}
