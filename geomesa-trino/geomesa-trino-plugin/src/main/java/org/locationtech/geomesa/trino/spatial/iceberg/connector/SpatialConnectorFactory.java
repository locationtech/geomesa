/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;
import org.locationtech.geomesa.trino.security.AuthorizationResolver;
import org.locationtech.geomesa.trino.security.FileAuthorizationResolver;

import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import org.locationtech.geomesa.trino.spatial.SpatialIcebergPlugin;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ConnectorFactory for the spatial_iceberg connector. Bootstraps Trino's
 * built-in IcebergConnectorFactory and wraps the resulting Connector with
 * {@link SpatialConnector} to inject spatial-predicate pushdown.
 *
 * <p>Geometry columns are discovered automatically via naming conventions: a
 * VARBINARY column X is treated as geometry if {@code __X_bbox__},
 * {@code __X_z2__}, or {@code __X_xz2__} exists in the same table.
 */
public class SpatialConnectorFactory implements ConnectorFactory {

    static final String CONNECTOR_NAME = "spatial_iceberg";

    /** Catalog-property prefix for Trino-layer row-visibility enforcement.
     *  These keys are consumed here and stripped before the config reaches the
     *  Iceberg factory (which rejects unknown properties). */
    private static final String SECURITY_PREFIX = "geomesa.security.";
    private static final String AUTH_RESOLVER   = SECURITY_PREFIX + "auth-resolver";
    private static final String AUTH_MAPPING    = SECURITY_PREFIX + "auth-mapping-file";


    /** Enables connector-side bbox filtering (see {@link BboxFilteringPageSource}).
     *  - For a rectangle {@code ST_Intersects} on a Z2/point geometry column the connector claims
     *    the predicate as enforced and the page source filters:
     *      a) rows whose bbox is inside the rectangle are accepted (skips decoding their geometry WKB),
     *      b) rows outside are rejected
     *      c) an exact test runs only on the envelope boundary.
     *  - ON by default;
     *  - Disable via {@code geomesa.spatial.bbox-page-filter=false}.
     */
    private static final String SPATIAL_PREFIX  = "geomesa.spatial.";
    private static final String BBOX_PAGE_FILTER = SPATIAL_PREFIX + "bbox-page-filter";

    /** Default constructor; instances are created by {@link SpatialIcebergPlugin}. */
    public SpatialConnectorFactory() {}

    /**
     * Returns the connector name registered with Trino.
     *
     * @return the connector name
     */
    @Override
    public String getName() {
        return CONNECTOR_NAME;
    }

    /**
     * Creates a spatial-iceberg connector by wrapping Trino's built-in Iceberg connector.
     *
     * @param catalogName the trino catalog name
     * @param config the catalog configuration
     * @param context the connector context
     * @return the spatial connector wrapping the iceberg connector
     */
    @Override
    public Connector create(String catalogName, Map<String, String> config,
                            ConnectorContext context) {
        AuthorizationResolver resolver = buildResolver(config);
        boolean bboxShortCircuit = Boolean.parseBoolean(config.getOrDefault(BBOX_PAGE_FILTER, "true"));

        // Iceberg uses strict config validation; strip our keys so it doesn't reject them as unused.
        Map<String, String> icebergConfig = new LinkedHashMap<>();
        config.forEach((k, v) -> {
            if (!k.startsWith(SECURITY_PREFIX) && !k.startsWith(SPATIAL_PREFIX)) icebergConfig.put(k, v);
        });

        ConnectorFactory icebergFactory = new IcebergPlugin().getConnectorFactories().iterator().next();
        Connector icebergConnector = icebergFactory.create(catalogName, icebergConfig, context);
        return new SpatialConnector(icebergConnector, catalogName, resolver, bboxShortCircuit);
    }

    /** Builds the identity→auths resolver from catalog config, or null when no
     *  {@code geomesa.security.*} key is set (Trino-layer enforcement is opt-in;
     *  absent config leaves behavior identical to before). */
    private static AuthorizationResolver buildResolver(Map<String, String> config) {
        boolean configured = config.keySet().stream().anyMatch(k -> k.startsWith(SECURITY_PREFIX));
        if (!configured) {
            return null;
        }
        String kind = config.getOrDefault(AUTH_RESOLVER, "file");
        if ("file".equals(kind)) {
            String path = config.get(AUTH_MAPPING);
            if (path == null) {
                throw new IllegalArgumentException(AUTH_RESOLVER + "=file requires " + AUTH_MAPPING);
            }
            return new FileAuthorizationResolver(Path.of(path));
        }
        // External implementation named by fully-qualified class with a (Map<String,String>) constructor.
        try {
            return (AuthorizationResolver) Class.forName(kind).getConstructor(Map.class).newInstance(config);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                "Cannot load AuthorizationResolver '" + kind + "' named by " + AUTH_RESOLVER, e
            );
        }
    }
}
