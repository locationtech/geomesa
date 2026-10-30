/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
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
import org.locationtech.geomesa.security.DelegatedIdentity;
import org.locationtech.geomesa.security.DelegatedAuthorizationsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Read-only GeoTools {@link ContentDataStore} backed by a Trino/Iceberg catalog, with Z2/XZ2
 * partition pruning and row-level visibility. The datastore is the trusted intermediary that
 * forwards each caller's authorizations to Trino per query as an extra credential.
 *
 * <p>The store uses {@code ContentDataStore} for its Iceberg schema discovery and custom query readers
 * (see {@link TrinoFeatureSource}). {@code JDBCDataStore} also supports read-only schemas, but adopting it
 * would require integrating these paths and per-request authorization with its SQL dialect and connection handling.
 * Filter simplification and splitting reuse GeoTools visitors independently of the datastore base class. Because the
 * forwarded authorizations are fixed on a connection at creation, connections are pooled keyed by the normalized
 * auth set ({@link ConnectionPool}): requests with the same effective auths reuse connections (see {@link #connect(List)}).
 * Delegation instead emits configured user/groups/auths headers on dedicated, unpooled connections.
 */
public class TrinoDataStore extends ContentDataStore {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoDataStore.class);
    private static final long TYPE_NAMES_TTL_MILLIS = TimeUnit.SECONDS.toMillis(60);
    private static final String JDBC_URL_PREFIX = "jdbc:trino://";
    private static final String JDBC_URL_FORMAT = JDBC_URL_PREFIX + "%s:%d/%s/%s";
    private static final String EXTRA_HEADERS_PROPERTY = "extraHeaders";
    private static final String HEADER_VALUE_SEPARATOR = ":";
    private static final String HEADER_ENTRY_SEPARATOR = ";";

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

    private boolean delegation;
    private String delegationUrl;
    private String userHeader;
    private String groupsHeader;
    private String authsHeader;

    /** Dedicated endpoint for delegated queries and metadata discovery. */
    void configureDelegation(boolean enabled, String url, String userHeader, String groupsHeader, String authsHeader) {
        if (url != null && !url.startsWith(JDBC_URL_PREFIX)) {
            throw new IllegalArgumentException("Delegation URL must be a Trino JDBC URL");
        }
        if (enabled) {
            this.userHeader = normalizeHeaderName(userHeader);
            this.groupsHeader = normalizeHeaderName(groupsHeader);
            this.authsHeader = normalizeHeaderName(authsHeader);
            if (new HashSet<>(List.of(this.userHeader, this.groupsHeader, this.authsHeader)).size() != 3) {
                throw new IllegalArgumentException("Delegated user, groups and auths header names must be distinct");
            }
        }
        delegation = enabled;
        delegationUrl = url == null ? String.format(JDBC_URL_FORMAT, host, port, catalog, trinoSchema) : url;
    }

    private static String normalizeHeaderName(String name) {
        if (name == null || !name.matches("[!#$%&'*+.^_`|~0-9A-Za-z-]+")) {
            throw new IllegalArgumentException("A valid delegation header name is required");
        }
        return name.toLowerCase(Locale.ROOT);
    }

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

    /** Metadata connection: delegated jobs retain their identity; background discovery
     *  uses the configured service user with no grants. */
    Connection connect() throws SQLException {
        if (delegation) {
            var identity = DelegatedAuthorizationsProvider.identity().orElse(null);
            return delegatedConnection(identity, identity == null ? null :
                (authProvider == null ? List.of() : authProvider.getAuthorizations()), true);
        }
        return pool.borrow(null);
    }

    /** Delegated identities use dedicated connections, never the authorization-only pool. */
    Connection connect(List<String> auths) throws SQLException {
        if (delegation) { return delegatedConnection(DelegatedAuthorizationsProvider.identity().orElse(null), auths, false); }
        return pool.borrow(auths);
    }

    private Connection delegatedConnection(DelegatedIdentity identity, List<String> auths, boolean metadata) throws SQLException {
        var selected = new LinkedHashMap<String, String>();
        if (metadata && identity == null) {
            selected.put(userHeader, user);
            selected.put(groupsHeader, "");
            selected.put(authsHeader, "");
        } else {
            if (identity == null) { throw new SQLException("Delegated identity is missing"); }
            selected.put(userHeader, identity.user());
            selected.put(groupsHeader, delegatedGrants(identity.groups().stream().sorted().toList()));
            // Data connections must honor any configured authorization ceiling.
            var effectiveAuths = auths == null ? identity.auths().stream().sorted().toList() : auths;
            selected.put(authsHeader, delegatedGrants(effectiveAuths));
        }
        String subject = selected.get(userHeader);
        if (subject.isBlank() || subject.chars().anyMatch(c -> c <= 32 || c > 126 || c == ';')) {
            throw new SQLException("Delegated user cannot be represented in JDBC headers");
        }
        for (String name : List.of(groupsHeader, authsHeader)) {
            String value = selected.get(name);
            // The receiving mapper must treat absent grants as empty; JDBC rejects empty property values.
            if (value.isEmpty()) { selected.remove(name); }
        }
        var props = new Properties();
        // Trino's session user must match the authenticated delegated identity.
        props.setProperty("user", subject);
        props.setProperty(EXTRA_HEADERS_PROPERTY, selected.entrySet().stream()
            .map(e -> e.getKey() + HEADER_VALUE_SEPARATOR + e.getValue()).collect(Collectors.joining(HEADER_ENTRY_SEPARATOR)));
        return openDelegatedConnection(delegationUrl, props);
    }

    private static String delegatedGrants(List<String> grants) throws SQLException {
        for (String grant : grants) {
            if (grant == null || grant.isEmpty() ||
                    grant.chars().anyMatch(c -> c <= 32 || c > 126 || c == ',' || c == ';')) {
                throw new SQLException("Delegated grant '" + grant + "' cannot be represented in JDBC headers");
            }
        }
        return String.join(",", grants);
    }

    protected Connection openDelegatedConnection(String url, Properties properties) throws SQLException {
        try {
            return DriverManager.getConnection(url, properties);
        } catch (SQLException | IllegalArgumentException e) {
            // Driver property errors may render every supplied header. Do not retain their cause.
            throw new SQLException("Unable to establish a connection with delegated identity");
        }
    }

    /** Opens a real connection for a normalized auth set (pool miss). */
    private Connection openConnection(List<String> normalizedAuths) throws SQLException {
        String url = String.format(JDBC_URL_FORMAT,
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
                    LOG.info("Suppressing non-Iceberg table '{}' as it does not have a '$properties' metadata table", tableName);
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
                        LOG.info("Suppressing table '{}' as it does not have expected property 'geomesa.sft.name'", tableName);
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
