package org.locationtech.geomesa.api;

import com.vividsolutions.jts.geom.Geometry;

import java.util.Date;

/**
 * GeoMesaIndex is an API for utilizing GeoMesa as a spatial index
 * without bringing in the Geotools SimpleFeature data model
 * @param <T>
 */
public interface GeoMesaIndex<T> {

    /**
     * Query a GeoMesa index
     * @param query
     * @return
     */
    Iterable<T> query(GeoMesaQuery query);

    /**
     * Put a value in the GeoMesa index
     * @param t
     * @param geometry
     * @param dtg
     */
    void put(T t, Geometry geometry, Date dtg);

    /**
     * Delete a value from the GeoMesa index
     * @param t
     */
    void delete(T t);

}
