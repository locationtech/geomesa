/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFactorySpi;
import org.locationtech.geomesa.security.AuthUtils;
import org.locationtech.geomesa.security.AuthorizationsProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * GeoTools {@link DataStoreFactorySpi} that creates read-only {@link TrinoDataStore}s backed by
 * Trino/Iceberg, discovered via the GeoTools {@code DataStoreFinder} service loader.
 */
public class TrinoDataStoreFactory implements DataStoreFactorySpi {

    /** Default constructor; instances are created by the GeoTools {@code DataStoreFinder} SPI. */
    public TrinoDataStoreFactory() {}

    /** Trino coordinator host (required). */
    public static final Param HOST =
        new Param("trino.host", String.class, "Trino host", true);
    /** Trino coordinator port (required). */
    public static final Param PORT =
        new Param("trino.port", Integer.class, "Trino port", true);
    /** Trino catalog; use {@code spatial_iceberg} for Z2 pruning (default {@code spatial_iceberg}). */
    public static final Param CATALOG =
        new Param("trino.catalog", String.class, "Trino catalog (use spatial_iceberg for Z2 pruning)",
            false, "spatial_iceberg");
    /** Trino schema (default {@code spatial}). */
    public static final Param SCHEMA =
        new Param("trino.schema", String.class, "Trino schema", false, "spatial");
    /** Namespace URI applied to type names (GeoServer-workspace support). */
    public static final Param NAMESPACE =
        new Param("namespace", String.class, "Namespace URI", false);
    /** Trino connection user (service account); must hold the full auth set when Trino-layer
     *  visibility enforcement is active on the catalog. */
    public static final Param USER =
        new Param("trino.user", String.class,
            "Trino connection user (service account). Must hold the full auth set "
                + "when Trino-layer visibility enforcement is active on the catalog.",
            false, TrinoDataStore.DEFAULT_USER);

    /** Comma-delimited superset of authorizations to use for queries. */
    public static final Param AUTHS = new Param("geomesa.security.auths", String.class,
        "Comma-delimited superset of authorizations to be used for queries", false);
    /** Explicit {@link AuthorizationsProvider} instance. */
    public static final Param AUTH_PROVIDER = new Param("geomesa.security.auths.provider",
        AuthorizationsProvider.class, "Explicit AuthorizationsProvider instance", false);
    /** Shared secret presented to Trino as the {@code secret} extra credential; must match the
     *  catalog's {@code geomesa.security.auths-secret}. No comma or colon. */
    public static final Param SECRET = new Param("geomesa.security.auths-secret", String.class,
        "Shared secret presented to Trino as the 'secret' extra credential; must match the "
            + "catalog's geomesa.security.auths-secret. No comma or colon.", false);

    private static final String TRINO_DRIVER_CLASS = "io.trino.jdbc.TrinoDriver";

    /**
     * Returns the human-readable display name of this datastore factory.
     *
     * @return display name
     */
    @Override public String getDisplayName()  { return "Trino (GeoMesa)"; }
    /**
     * Returns a human-readable description of this datastore factory.
     *
     * @return description
     */
    @Override public String getDescription()  { return "Read-only GeoTools DataStore backed by Trino/Iceberg with Z2 partition pruning"; }
    /**
     * Returns the connection parameters this factory accepts.
     *
     * @return parameter descriptors
     */
    @Override public Param[] getParametersInfo() {
        return new Param[]{HOST, PORT, SCHEMA, USER, AUTHS, SECRET, NAMESPACE};
    }

    /**
     * Whether this factory can process the given connection parameters.
     *
     * @param params connection parameters
     * @return true if host and port are present
     */
    @Override
    public boolean canProcess(Map<String, ?> params) {
        return params.containsKey(HOST.key) && params.containsKey(PORT.key);
    }

    /**
     * Creates a datastore from the given connection parameters.
     *
     * @param params connection parameters
     * @return a configured read-only Trino datastore
     */
    @Override
    public DataStore createDataStore(Map<String, ?> params) throws IOException {
        String host    = (String)  HOST.lookUp(params);
        int    port    = (Integer) PORT.lookUp(params);
        String catalog = lookUpOrDefault(CATALOG, params);
        String schema  = lookUpOrDefault(SCHEMA, params);
        String user    = lookUpOrDefault(USER, params);

        String authsStr = (String) AUTHS.lookUp(params);
        List<String> auths;
        if (authsStr == null || authsStr.isBlank()) {
            auths = List.of();
        } else {
            auths = Arrays.stream(authsStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
        }
        AuthorizationsProvider authProvider = AuthUtils.getProvider(params, auths);
        String secret = (String) SECRET.lookUp(params);
        TrinoDataStore ds =
            new TrinoDataStore(host, port, catalog, schema, authProvider, user, secret);
        String namespace = (String) NAMESPACE.lookUp(params);
        if (namespace != null && !namespace.isBlank()) {
            ds.setNamespaceURI(namespace);
        }
        return ds;
    }

    @SuppressWarnings("unchecked")
    private static <T> T lookUpOrDefault(Param param, Map<String, ?> params) throws IOException {
        T value = (T) param.lookUp(params);
        return value != null ? value : (T) param.getDefaultValue();
    }

    /**
     * Creating a new datastore is unsupported; this datastore is read-only.
     *
     * @param params connection parameters
     * @return never returns normally
     */
    @Override
    public DataStore createNewDataStore(Map<String, ?> params) throws IOException {
        throw new UnsupportedOperationException("TrinoDataStore is read-only");
    }

    /**
     * Whether the Trino JDBC driver is on the classpath.
     *
     * @return true if the Trino JDBC driver is available
     */
    @Override
    public boolean isAvailable() {
        try {
            Class.forName(TRINO_DRIVER_CLASS);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
