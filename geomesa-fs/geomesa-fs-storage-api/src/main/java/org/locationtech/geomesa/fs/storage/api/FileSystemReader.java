package org.locationtech.geomesa.fs.storage.api;

import org.geotools.data.Query;
import org.opengis.feature.simple.SimpleFeature;

import java.util.Iterator;

/**
 * Created by anthony on 5/29/17.
 */
public interface FileSystemReader {

    Iterator<SimpleFeature> filterFeatures(Query q);
}
