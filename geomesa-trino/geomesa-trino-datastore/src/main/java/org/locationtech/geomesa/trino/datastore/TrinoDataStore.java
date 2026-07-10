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
import org.locationtech.geomesa.security.AuthorizationsProvider;

import java.io.IOException;
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
 * offers per-request-authenticated connections per query and visibility/entitlement filtering. A consequence is that
 * there is no shared connection pool: each query opens its own connection so the forwarded authorizations always
 * reflect the current caller context (see {@link #connect(List)}).
 */
public class TrinoDataStore extends ContentDataStore {

    private static final long TYPE_NAMES_TTL_NANOS = TimeUnit.SECONDS.toNanos(60);

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

    private final Object typeNamesLock = new Object();
    private List<Name> cachedTypeNames;
    private long cachedTypeNamesExpiry;

    TrinoDataStore(String host, int port, String catalog, String schema,
                   AuthorizationsProvider authProvider, String user, String secret) {
        this.host = host;
        this.port = port;
        this.catalog = catalog;
        this.trinoSchema = schema;
        this.authProvider = authProvider;
        this.user = user;
        this.secret = secret;
        setNamespaceURI("http://geomesa.locationtech.org");
    }

    /** Null when no security params were supplied (filtering disabled). */
    AuthorizationsProvider authProvider() { return authProvider; }

    /** Metadata connection (no caller auths) — for table listing against
     *  information_schema, which the connector never row-filters. */
    Connection connect() throws SQLException {
        return connect(null);
    }

    /** Opens a per-query Trino connection, forwarding the caller's authorizations
     *  as a Trino extra credential. The {@code spatial_iceberg} connector's
     *  ExtraCredentialAuthorizationResolver reads it to build the row-level
     *  visibility filter for THIS request. A fresh connection is opened per query,
     *  so the credential always reflects the current per-request auths. When
     *  {@code auths} is null/empty no credential is sent (the caller then sees only
     *  unrestricted rows — fail-closed). */
    Connection connect(List<String> auths) throws SQLException {
        String url = String.format("jdbc:trino://%s:%d/%s/%s",
            host, port, catalog, trinoSchema);
        return DriverManager.getConnection(url, connectionProperties(user, auths, secret));
    }

    /** Builds the JDBC connection properties. Extracted so the extra-credential
     *  encoding is unit-testable without opening a real connection.
     *
     *  <p>The Trino JDBC {@code extraCredentials} property is {@code name:value}
     *  pairs delimited by SEMICOLONS (NOT commas), and each value must be printable
     *  ASCII with no spaces. So auth tokens are joined with PIPES and the secret is
     *  a second pair: {@code auths:basic|privileged;secret:<value>}. Throws
     *  {@code IllegalArgumentException} on a token containing a transport delimiter —
     *  the server-side resolver would re-split it into auths that were never issued
     *  (see {@link AuthTokens}). */
    static Properties connectionProperties(String user, List<String> auths, String secret) {
        AuthTokens.validate(auths);
        Properties props = new Properties();
        props.setProperty("user", user);
        StringBuilder creds = new StringBuilder();
        if (auths != null && !auths.isEmpty()) {
            creds.append(AUTHS_CREDENTIAL).append(':').append(String.join("|", auths));
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
            if (cachedTypeNames != null && System.nanoTime() - cachedTypeNamesExpiry < 0) {
                return cachedTypeNames;
            }
            cachedTypeNames = queryTypeNames();
            cachedTypeNamesExpiry = System.nanoTime() + TYPE_NAMES_TTL_NANOS;
            return cachedTypeNames;
        }
    }

    private List<Name> queryTypeNames() throws IOException {
        String sql = "SELECT table_name FROM " + escapeQuotes(catalog)
            + ".information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'";
        List<Name> names = new ArrayList<>();
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, trinoSchema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    names.add(name(rs.getString("table_name")));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to list tables", e);
        }
        return Collections.unmodifiableList(names);
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
