package org.locationtech.geomesa.api;

import com.vividsolutions.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.Date;

public interface SimpleFeatureView<T> {

    void populate(SimpleFeature f, T t, String id, byte[] payload, Geometry geom, Date dtg);

    SimpleFeatureType getSimpleFeatureType();

}
