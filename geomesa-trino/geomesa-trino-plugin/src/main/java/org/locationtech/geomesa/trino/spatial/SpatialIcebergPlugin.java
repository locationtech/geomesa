/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial;
import org.locationtech.geomesa.trino.security.GeoMesaSecurityFunctions;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialConnectorFactory;

import java.util.List;
import java.util.Set;

/**
 * Trino Plugin for the spatial-iceberg connector.
 * Registered via META-INF/services/io.trino.spi.Plugin.
 */
public class SpatialIcebergPlugin implements Plugin {

    /** Default constructor; invoked by Trino's plugin loader. */
    public SpatialIcebergPlugin() {}

    /**
     * Returns the connector factories provided by this plugin.
     *
     * @return the spatial-iceberg connector factory
     */
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return List.of(new SpatialConnectorFactory());
    }

    /**
     * Returns the SQL functions provided by this plugin.
     *
     * @return the GeoMesa security functions
     */
    @Override
    public Set<Class<?>> getFunctions() {
        return Set.of(GeoMesaSecurityFunctions.class);
    }
}
