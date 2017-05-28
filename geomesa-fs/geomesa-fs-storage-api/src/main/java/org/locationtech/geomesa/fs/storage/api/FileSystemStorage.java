package org.locationtech.geomesa.fs.storage.api;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.util.Iterator;

public interface FileSystemStorage {
    SimpleFeatureType getSimpleFeatureType();

    Iterator<SimpleFeature> query(Filter f);
}
