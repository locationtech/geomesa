package org.locationtech.geomesa.api;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.classification.InterfaceStability;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.Date;

@InterfaceStability.Unstable
public interface SimpleFeatureView<T> {

    IndexType indexType();

    SimpleFeatureType buildSimpleFeatureType();

    SimpleFeature buildSimpleFeature(T t, Geometry geometry);

    SimpleFeature buildSimpleFeature(T t, Geometry geometry, Date date);
}
