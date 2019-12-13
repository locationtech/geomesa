/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.api;

import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Date;
import java.util.Map;

/**
 * GeoMesaIndex is an API for utilizing GeoMesa as a spatial index
 * without bringing in the Geotools SimpleFeature data model
 * @param <T>
 */
@InterfaceStability.Unstable
@Deprecated
public interface GeoMesaIndex<T> {

    IndexType[] supportedIndexes();

    /**
     * Query a GeoMesa index
     * @param query
     * @return
     */
    Iterable<T> query(GeoMesaQuery query);

    /**
     * Insert a value in the GeoMesa index
     * @param value
     * @param geometry
     * @param dtg date time of the object or null if using spatial only
     * @return identifier of the object stored
     */
    String insert(T value, Geometry geometry, Date dtg);

    /**
     * Insert a value in the GeoMesa index
     * @param id identifier to use for the value
     * @param value
     * @param geometry
     * @param dtg date time of the object or null if using spatial only
     * @return identifier of the object stored
     */
    String insert(String id, T value, Geometry geometry, Date dtg);

    /**
     * Insert a value in the GeoMesa index
     * @param id identifier to use for the value
     * @param value
     * @param geometry
     * @param dtg date time of the object or null if using spatial only
     * @param hints implementation specific hints for serialization
     * @return The identifier of the value written to GeoMesa
     */
    String insert(String id, T value, Geometry geometry, Date dtg, Map<String, Object> hints);

    /**
     * Update a given identifier with a new value
     * @param id
     * @param newValue
     * @param geometry
     * @param dtg
     */
    void update(String id, T newValue, Geometry geometry, Date dtg);

    /**
     * Delete a value from the GeoMesa index
     * @param id
     */
    void delete(String id);

    /**
     * Removes the entire index from GeoMesa.
     */
    void removeSchema();

    /**
     * Flushes any pending transactions
     */
    void flush();

    /**
     * Closes the index and flushes any pending commits
     */
    void close();

    String VISIBILITY = "visibility";
}
